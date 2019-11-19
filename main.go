package main

import (
	"os"

	"encoding/hex"
	"errors"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/history"
	"github.com/leijurv/gb/paranoia"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/utils"
	"github.com/urfave/cli"
)

func main() {
	db.SetupDatabase()
	defer db.ShutdownDatabase()

	app := cli.NewApp()
	app.Name = "gb"
	app.Usage = "backup the files"
	app.Commands = []cli.Command{
		{
			Name:  "backup",
			Usage: "backup a directory",
			Action: func(c *cli.Context) error {
				path := c.Args().First()
				if path == "" {
					return errors.New("Must give me a path to backup. Use \".\" for current directory.")
				}
				if len(storage.GetAll()) == 0 {
					return errors.New("make a storage first")
				}
				backup.BackupADirectoryRecursively(path)
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
			Usage: "yeah you SAY you backed up the files but how do i KNOW",
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
							},
							Action: func(c *cli.Context) error {
								if c.String("label") == "" {
									return errors.New("give me a label")
								}
								if c.String("bucket") == "" {
									return errors.New("give me a bucket")
								}
								if c.String("path") == "" {
									return errors.New("give me a path")
								}
								storage.NewS3Storage(c.String("label"), c.String("bucket"), c.String("path"))
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
	}
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}
