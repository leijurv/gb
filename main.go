package main

import (
	"log"
	"os"

	"errors"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/urfave/cli"
)

func main() {
	db.SetupDatabase()
	defer db.ShutdownDatabase()

	app := cli.NewApp()
	app.Commands = []cli.Command{
		{
			Name: "backup",
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
			Name: "paranoia",
			Action: func(c *cli.Context) error {
				if len(storage.GetAll()) == 0 {
					return errors.New("make a storage first")
				}
				testAll()
				return nil
			},
		},
		{
			Name: "storage",
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
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
