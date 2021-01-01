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
	"github.com/leijurv/gb/history"
	"github.com/leijurv/gb/paranoia"
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
			Action: func(c *cli.Context) error {
				path := c.Args().First()
				if len(storage.GetAll()) == 0 {
					return errors.New("make a storage first")
				}
				backup.Backup(path)
				return nil
			},
		},
		{
			Name:  "cat",
			Usage: "dump a file to stdout by its sha256. always fetches from storage, never uses your filesystem",
			Action: func(c *cli.Context) error {
				data, err := hex.DecodeString(c.Args().First())
				if err != nil {
					return err
				}
				if len(data) != 32 {
					return errors.New("wrong length")
				}
				utils.Copy(os.Stdout, download.CatEz(data))
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
					Action: func(c *cli.Context) error {
						if len(storage.GetAll()) == 0 {
							return errors.New("make a storage first")
						}
						paranoia.TestAllFiles()
						return nil
					},
				},
				{
					Name:  "storage",
					Usage: "fetch all metadata (aka: list all blobs) in storage and ensure their size and checksum is what we expect",
					Action: func(c *cli.Context) error {
						if len(storage.GetAll()) == 0 {
							return errors.New("make a storage first")
						}
						paranoia.StorageParanoia()
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
							},
							Action: func(c *cli.Context) error {
								for _, thing := range []string{"label", "bucket", "path", "region", "keyid", "secretkey"} {
									if c.String(thing) == "" {
										return errors.New("give me a " + thing)
									}
								}
								storage.NewS3Storage(c.String("label"), c.String("bucket"), c.String("path"), c.String("region"), c.String("keyid"), c.String("secretkey"))
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
				var timestamp int64
				if c.String("since") != "" {
					t, err := dateparse.ParseLocal(c.String("since"))
					if err != nil {
						log.Println("Hint: make sure you are providing a year")
						return err
					}
					timestamp = t.Unix()
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
			},
			Action: func(c *cli.Context) error {
				var timestamp int64
				if c.String("at") != "" {
					t, err := dateparse.ParseLocal(c.String("at"))
					if err != nil {
						log.Println("Hint: make sure you are providing a year")
						return err
					}
					timestamp = t.Unix()
					// restore prints out the timestamp for confirmation, no need to do it twice
				}
				download.Restore(c.Args().Get(0), c.Args().Get(1), timestamp)
				return nil
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}
