module github.com/leijurv/gb

go 1.13

require (
	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	cloud.google.com/go v0.102.0 // indirect
	github.com/DataDog/zstd v1.5.2
	github.com/araddon/dateparse v0.0.0-20210429162001-6b43995a97de
	github.com/aws/aws-sdk-go v1.44.26
	github.com/cespare/diff v0.1.0
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/mattn/go-sqlite3 v1.11.0 // database locking was changed in newer versions. gb will randomly panic with "database is locked" in multithreaded situations if this dependency is updated further, apparently in disregard of the option "_busy_timeout=20000" that is set in the db connection. sorry.
	github.com/muja/goconfig v0.0.0-20180417074348-0a635507dddc // indirect
	github.com/sabhiram/go-gitignore v0.0.0-20210923224102-525f6e181f06 // indirect
	github.com/tyler-smith/go-bip39 v1.1.0
	github.com/urfave/cli v1.22.9
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/net v0.0.0-20220531201128-c960675eff93
	golang.org/x/oauth2 v0.0.0-20220524215830-622c5d57e401
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/api v0.82.0
	google.golang.org/genproto v0.0.0-20220601144221-27df5f98adab // indirect
	google.golang.org/grpc v1.47.0 // indirect
)
