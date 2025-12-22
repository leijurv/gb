module github.com/leijurv/gb

go 1.21

require (
	github.com/mattn/go-sqlite3 v1.11.0 // database locking was changed in newer versions. gb will randomly panic with "database is locked" in multithreaded situations if this dependency is updated further, apparently in disregard of the option "_busy_timeout=20000" that is set in the db connection. sorry.

	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	github.com/DataDog/zstd v1.5.2
	github.com/araddon/dateparse v0.0.0-20210429162001-6b43995a97de
	github.com/aws/aws-sdk-go-v2 v1.32.7
	github.com/aws/aws-sdk-go-v2/credentials v1.17.49
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.17.46
	github.com/aws/aws-sdk-go-v2/service/s3 v1.72.1
	github.com/tyler-smith/go-bip39 v1.1.0
	github.com/urfave/cli v1.22.9
	golang.org/x/net v0.0.0-20220531201128-c960675eff93
	golang.org/x/oauth2 v0.0.0-20220524215830-622c5d57e401
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a
	google.golang.org/api v0.82.0
)

require (
	cloud.google.com/go/compute v1.6.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.26 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.4.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.18.7 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/googleapis/gax-go/v2 v2.4.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220601144221-27df5f98adab // indirect
	google.golang.org/grpc v1.47.0 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
)
