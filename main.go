package main

import (
	"encoding/hex"
	"errors"
	"log"
	"os"
	"time"

	"github.com/araddon/dateparse"
	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/dupes"
	"github.com/leijurv/gb/gbfs"
	"github.com/leijurv/gb/history"
	"github.com/leijurv/gb/paranoia"
	"github.com/leijurv/gb/proxy"
	"github.com/leijurv/gb/repack"
	"github.com/leijurv/gb/replicate"
	"github.com/leijurv/gb/share"
	"github.com/leijurv/gb/stats"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/utils"
	"github.com/urfave/cli"
)

func main() {
	defer db.ShutdownDatabase()

	app := cli.NewApp()
	app.Name = "gb"
	app.Usage = "backup your files"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "config-file",
			Value:       config.HomeDir + "/.gb.conf",
			Usage:       "path to where you want your config file",
			Destination: &config.ConfigLocation,
		},
		&cli.StringFlag{
			Name:        "database-file",
			Usage:       "path to where the database file is (overrides path from config file)",
			Destination: &config.DatabaseLocation,
		},
		&cli.BoolFlag{
			Name:  "no-log-timestamps",
			Usage: "do not include timestamps in logs",
		},
	}
	app.Before = func(c *cli.Context) error {
		if c.Bool("no-log-timestamps") {
			log.SetFlags(0)
		}

		// we don't know where the database should be read from until after the "config-file" flag is parsed
		db.SetupDatabase()
		return nil
	}
	app.Commands = []cli.Command{
		{
			Name:  "backup",
			Usage: "backup a directory (or file)",
			Flags: []cli.Flag{&cli.BoolFlag{
				Name:  "no-backup-database",
				Usage: "do not upload the database",
			}},
			Action: func(c *cli.Context) error {
				if len(storage.GetAll()) == 0 {
					return errors.New("make a storage first")
				}
				paths := append([]string{c.Args().First()}, c.Args().Tail()...) // even if no argument (like: "gb backup"), backup current directory by passing one empty string arg
				backup.Backup(paths)
				if !c.Bool("no-backup-database") {
					backup.BackupDB()
				}
				return nil
			},
		},
		{
			Name:  "cat",
			Usage: "dump a file to stdout by its sha256. always fetches from storage, never uses your filesystem",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
			},
			Action: func(c *cli.Context) error {
				stor, ok := storage.StorageSelect(c.String("label"))
				if !ok {
					return nil
				}
				data, err := hex.DecodeString(c.Args().First())
				if err != nil {
					return err
				}
				if len(data) != 32 {
					return errors.New("wrong length")
				}
				utils.Copy(os.Stdout, download.CatEz(data, stor))
				return nil
			},
		},
		{
			Name:  "paranoia",
			Usage: "yeah you SAY you backed up the files but how do i KNOW (you can also directly put a path/to/file instead of files/storage/db)",
			Subcommands: []cli.Command{
				{
					Name:  "files",
					Usage: "download files and calculate their hashes",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "label",
							Usage: "storage label",
						},
					},
					Action: func(c *cli.Context) error {
						if len(storage.GetAll()) == 0 {
							return errors.New("make a storage first")
						}
						paranoia.TestAllFiles(c.String("label"))
						return nil
					},
				},
				{
					Name:  "storage",
					Usage: "fetch all metadata (aka: list all blobs) in storage and ensure their size and checksum is what we expect",
					Flags: []cli.Flag{
						&cli.BoolFlag{
							Name:  "delete-unknown-files",
							Usage: "delete any files found in storage that are not in the local database",
						},
					},
					Action: func(c *cli.Context) error {
						if len(storage.GetAll()) == 0 {
							return errors.New("make a storage first")
						}
						paranoia.StorageParanoia(c.Bool("delete-unknown-files"))
						return nil
					},
				},
				{
					Name:  "db",
					Usage: "make sure the db is internally consistent",
					Action: func(c *cli.Context) error {
						paranoia.DBParanoia()
						return nil
					},
				},
				{
					Name:  "blob",
					Usage: "fetch blobs from storage and ensure that all contents are correct",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "label",
							Usage: "storage label",
						},
					},
					Action: func(c *cli.Context) error {
						paranoia.BlobParanoia(c.String("label"))
						return nil
					},
				},
			},
			Action: func(c *cli.Context) error {
				path := c.Args().First()
				if path == "" {
					return errors.New("Must give me a path to paranoia. Use \".\" for current directory. You can also `paranoia db` or `paranoia storage` or `paranoia files`.")
				}
				paranoia.ParanoiaFile(path)
				return nil
			},
		},
		{
			Name:  "storage",
			Usage: "where do i store the data",
			Subcommands: []cli.Command{
				{
					Name: "add",
					Subcommands: []cli.Command{
						{
							Name: "s3",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "label, l",
									Usage: "human readable label, can be anything",
								},
								cli.StringFlag{
									Name:  "bucket, b",
									Usage: "s3 bucket",
								},
								cli.StringFlag{
									Name:  "path, p",
									Usage: "path in the bucket, just put / if you want gb to write to the root",
								},
								cli.StringFlag{
									Name:  "region, r",
									Usage: "AWS region of your bucket, e.g. us-east-1",
								},
								cli.StringFlag{
									Name:  "keyid",
									Usage: "AWS key id (the shorter one)",
								},
								cli.StringFlag{
									Name:  "secretkey",
									Usage: "AWS secret key (the longer one)",
								},
								cli.StringFlag{
									Name:  "endpoint",
									Usage: "Override the s3 endpoint to another, for example you could put: backblazeb2.com",
								},
							},
							Action: func(c *cli.Context) error {
								for _, thing := range []string{"label", "bucket", "path", "region", "keyid", "secretkey"} {
									if c.String(thing) == "" {
										return errors.New("give me a " + thing)
									}
								}
								storage.NewS3Storage(c.String("label"), c.String("bucket"), c.String("path"), c.String("region"), c.String("keyid"), c.String("secretkey"), c.String("endpoint"))
								return nil
							},
						},
						{
							Name: "gdrive",
							Flags: []cli.Flag{
								cli.StringFlag{
									Name:  "label, l",
									Usage: "human readable label, can be anything",
								},
							},
							Action: func(c *cli.Context) error {
								if c.String("label") == "" {
									return errors.New("give me a label")
								}
								storage.NewGDriveStorage(c.String("label"))
								return nil
							},
						},
					},
				},
			},
		},
		{
			Name:  "history",
			Usage: "give revision history of a specific file (not a directory)",
			Action: func(c *cli.Context) error {
				history.FileHistory(c.Args().First())
				return nil
			},
		},
		{
			Name:  "search",
			Usage: "search for any path containing the given argument",
			Action: func(c *cli.Context) error {
				history.Search(c.Args().First())
				return nil
			},
		},
		{
			Name:  "ls",
			Usage: "list backup info about files in a directory",
			Action: func(c *cli.Context) error {
				history.DirHistory(c.Args().First())
				return nil
			},
		},
		{
			Name:  "mnemonic",
			Usage: "print out database encryption key mnemonic",
			Action: func(c *cli.Context) error {
				backup.Mnemonic(backup.DBKey())
				return nil
			},
		},
		{
			Name:  "fdupes",
			Usage: "print out duplicated file paths in fdupes format, for consumption by duperemove",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "since",
					Usage: "timestamp of the most recent successful and completed deduplication, so that the output can be filtered to only groups that contain files that were updated since then",
				},
			},
			Action: func(c *cli.Context) error {
				timestamp, err := parseTimestamp(c.String("at"))
				if err != nil {
					return err
				}
				if c.String("since") != "" {
					log.Println("Interpreting provided date as:", time.Unix(timestamp, 0).Format(time.RFC3339)) // so as to not misrepresent what will happen, this conversion intentionally rounds to nearest second
				}
				dupes.PrintDupes(timestamp)
				return nil
			},
		},
		{
			Name:  "restore",
			Usage: "restore your files =O",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "at, to, timestamp",
					Usage: "timestamp to which this should be restored",
				},
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
			},
			Action: func(c *cli.Context) error {
				stor, ok := storage.StorageSelect(c.String("label"))
				if !ok {
					return nil
				}
				timestamp, err := parseTimestamp(c.String("at"))
				if err != nil {
					return err
				}
				// restore prints out the timestamp for confirmation, no need to do it twice
				download.Restore(c.Args().Get(0), c.Args().Get(1), timestamp, stor)
				return nil
			},
		},
		{
			Name:  "sha256",
			Usage: "sha256 something",
			Action: func(c *cli.Context) error {
				log.SetFlags(0)
				hs := utils.NewSHA256HasherSizer()
				utils.Copy(&hs, os.Stdin)
				log.Println(hex.EncodeToString(hs.Hash()))
				return nil
			},
		},
		{
			Name:  "proxy",
			Usage: "proxy",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
				cli.StringFlag{
					Name:  "base",
					Usage: "base path",
				},
				cli.StringFlag{
					Name:  "listen",
					Usage: "ip and port to listen on",
					Value: "127.0.0.1:7893",
				},
				cli.BoolFlag{
					Name:  "iunderstandthisisnotauthenticated",
					Usage: "confirm this is notauthenticated",
				},
			},
			Action: func(c *cli.Context) error {
				if !c.Bool("iunderstandthisisnotauthenticated") {
					log.Println("This command is NOT authenticated. It allows ANYONE who can connect to " + c.String("listen") + " access to browse and download your files. Confirm this by adding the option `--iunderstandthisisnotauthenticated`")
					log.Println("To share individual files in an authenticated public-facing way, consider `gb share` and `gb shared` instead")
					return nil
				}
				proxy.Proxy(c.String("label"), c.String("base"), c.String("listen"))
				return nil
			},
		},
		{
			Name:  "restoredb",
			Usage: "restore an encrypted and compressed database backup",
			Action: func(c *cli.Context) error {
				download.RestoreDB(c.Args().First())
				return nil
			},
		},
		{
			Name:  "replicate",
			Usage: "replicate",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
			},
			Action: func(c *cli.Context) error {
				replicate.ReplicateBlobs(c.String("label"))
				return nil
			},
		},
		{
			Name:  "repack",
			Usage: "repack blobs (read blob IDs from stdin)",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
			},
			Action: func(c *cli.Context) error {
				repack.Repack(c.String("label"), repack.BlobIDsFromStdin)
				return nil
			},
		},
		{
			Name:  "deduplicate",
			Usage: "detect blobs that have duplicated entries and repack them so that all your blob entries have unique contents",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
			},
			Action: func(c *cli.Context) error {
				repack.Repack(c.String("label"), repack.Deduplicate)
				return nil
			},
		},
		{
			Name:  "upgrade-encryption",
			Usage: "find blobs that contain multiple files and use old style encryption, and repack them with unique encryption keys for each entry",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
			},
			Action: func(c *cli.Context) error {
				repack.Repack(c.String("label"), repack.UpgradeEncryption)
				return nil
			},
		},
		{
			Name:  "mount",
			Usage: "mount a readonly FUSE filesystem",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "at, timestamp",
					Usage: "timestamp files should be chosen from",
				},
				cli.StringFlag{
					Name:  "path",
					Usage: "source path where files come from",
					Value: "/",
				},
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
			},
			Action: func(c *cli.Context) error {
				stor, ok := storage.StorageSelect(c.String("label"))
				if !ok {
					return nil
				}
				timestamp, err := parseTimestamp(c.String("at"))
				if err != nil {
					return err
				}
				if timestamp == 0 {
					timestamp = time.Now().Unix()
				}
				gbfs.Mount(c.Args().First(), c.String("path"), timestamp, stor)
				return nil
			},
		},
		{
			Name:  "stat",
			Usage: "stat existing files and count how many files are not backed up",
			Action: func(c *cli.Context) error {
				paths := append([]string{c.Args().First()}, c.Args().Tail()...) // even if no argument (like: "gb backup"), backup current directory by passing one empty string arg
				backup.DryBackup(paths)
				return nil
			},
		},
		{
			Name:  "shared",
			Usage: "run a server that fulfills requests for files shared with `gb share`. files are served proxied from storage, not locally",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "label",
					Usage: "storage label",
				},
				cli.StringFlag{
					Name:  "listen",
					Usage: "ip and port to listen on",
					Value: ":7894",
				},
			},
			Action: func(c *cli.Context) error {
				share.Shared(c.String("label"), c.String("listen"))
				return nil
			},
		},
		{
			Name:  "share",
			Usage: "create a shareable url for a file or hash",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "name",
					Usage: "override the filename",
					Value: "",
				},
			},
			Action: func(c *cli.Context) error {
				share.CreateShareURL(c.Args().First(), c.String("name"))
				return nil
			},
		},
		{
			Name:  "stats",
			Usage: "show comprehensive backup statistics",
			Action: func(c *cli.Context) error {
				stats.ShowStats()
				return nil
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

func parseTimestamp(timestamp string) (int64, error) {
	if timestamp != "" {
		t, err := dateparse.ParseLocal(timestamp)
		if err != nil {
			log.Println("Hint: make sure you are providing a year")
			return 0, err
		}
		return t.Unix(), nil
	}
	return 0, nil
}
