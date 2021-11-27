module github.com/leijurv/gb

go 1.13

require (
	github.com/mattn/go-sqlite3 v1.11.0 // database locking was changed in newer versions. gb will randomly panic with "database is locked" in multithreaded situations if this dependency is updated further, apparently in disregard of the option "_busy_timeout=20000" that is set in the db connection. sorry.

	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05 // indirect
	github.com/DataDog/zstd v1.4.8
	github.com/araddon/dateparse v0.0.0-20210429162001-6b43995a97de
	github.com/aws/aws-sdk-go v1.41.11
	github.com/cespare/diff v0.1.0
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/tyler-smith/go-bip39 v1.1.0
	github.com/urfave/cli v1.22.5
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/net v0.0.0-20211020060615-d418f374d309
	golang.org/x/oauth2 v0.0.0-20211005180243-6b3c2da341f1
	golang.org/x/sys v0.0.0-20211025201205-69cdffdb9359
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/api v0.59.0
	google.golang.org/genproto v0.0.0-20211026145609-4688e4c4e024 // indirect
	google.golang.org/grpc v1.41.0 // indirect
)
