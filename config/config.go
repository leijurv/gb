package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var HomeDir = os.Getenv("HOME")
var ConfigLocation string
var DatabaseLocation string
var inited = false

type ConfigData struct {
	MinBlobSize            int64    `json:"min_blob_size"`
	MinCompressSize        int64    `json:"min_compress_size"`
	DatabaseLocation       string   `json:"database_location"`
	PaddingMinBytes        int64    `json:"padding_min_bytes"`
	PaddingMaxBytes        int64    `json:"padding_max_bytes"`
	PaddingMinPercent      float64  `json:"padding_min_percent"`
	PaddingMaxPercent      float64  `json:"padding_max_percent"`
	NumHasherThreads       int      `json:"num_hasher_threads"`
	NumUploaderThreads     int      `json:"num_uploader_threads"`
	UploadStatusInterval   int      `json:"upload_status_print_interval"`
	RelayServer            string   `json:"relay_server"`
	RelayServerPort        int      `json:"relay_server_port"`
	NoCompressionExts      []string `json:"no_compression_exts"`
	Includes               []string `json:"includes"`
	ExcludeSuffixes        []string `json:"exclude_suffixes"`
	ExcludePrefixes        []string `json:"exclude_prefixes"`
	DedupeExclude          []string `json:"dedupe_exclude"`
	IgnorePermissionErrors bool     `json:"ignore_permission_errors"`
	ShareBaseURL           string   `json:"share_base_url"`
	DisableLepton          bool     `json:"disable_lepton"`
	SkipHashFailures       bool     `json:"skip_hash_failures"`
	UseGitignore           bool     `json:"use_gitignore"`
}

func Config() ConfigData {
	begin()
	return config
}

var config = ConfigData{
	MinBlobSize:          64000000,
	MinCompressSize:      1024,
	DatabaseLocation:     HomeDir + "/.gb.db",
	PaddingMinBytes:      5021,
	PaddingMaxBytes:      12345,
	PaddingMinPercent:    0.05,
	PaddingMaxPercent:    0.1, // percent means percent. this is 0.1% not 10%!!
	NumHasherThreads:     2,
	NumUploaderThreads:   8,
	UploadStatusInterval: 5, // interval between "Bytes written:" prints, in seconds [-1 to disable prints]
	RelayServer:          "localhost",
	RelayServerPort:      -1,
	NoCompressionExts: []string{
		"mp4",
		"mkv",
		"png",
		"avi",
		"mov",
		"m4v",
		"mp3",
		"zip",
		"flac",
		"tiff",
		"tif",
		"m4a",
		"7z",
		"gz",
		"tgz",
		"jar",
		"torrent",
		"arw",
		"webm",
		"smi",
		"mpg",
		"m4p",
		"itlp",
		"aifc",
		"heic",
		"heif",
		"avif",
		"bz2",
		"bzp2",
		"bzip2",
		"xz",
		"zst",
		"aes",
		"gpg",
		"aac",
		"opus",
		"ogg",
		"wmv",
		"rar",
		"dmg",
	},
	Includes: []string{
		// folders that will be searched from if they are a child of the path argument.
		// useful if you want to backup a few sibling folders but not everything around them and want to do so by running backup on the parent folder.
		// this is ignored if no folders are given
		"/",
	},
	// if any component of the path matches these suffixes, it will be excluded, e.g. ".app"s
	ExcludeSuffixes: []string{
		".part",
	},
	ExcludePrefixes: []string{
		// e.g.
		// "/path/to/dir/to/exclude/",
		// you REALLY SHOULD include the trailing /
		// this really is just a starts with / ends with check on the path!
	},
	DedupeExclude: []string{
		// folders that you have already fully deduped against each other
		// if you backup a folder, then complete a full dedupe, you should add that folder to this list (at least, until you change its contents)
	},
	IgnorePermissionErrors: false,
	ShareBaseURL:           "",
	DisableLepton:          false,
	SkipHashFailures:       false,
	UseGitignore:           false,
}

/*
extensions that i thought about marking as no compress but decided against (these will be compressed):
exe
iso
pdf
docx
xlsx
pptx
wav
aif
aiff
idx
sub
srt
ass
sldprt
epub
tar
ico
log
itl
cue
ipa
m3u
m3u8
zim
class
*/

func begin() {
	if inited {
		return
	}
	inited = true
	if ConfigLocation == "" {
		panic("you can't call config in an init (before we parse our cli args), sorry!")
	}
	//log.Println("Assuming your home directory is " + HomeDir)
	//log.Println("Therefore I'm going to assume my config file should be at " + ConfigLocation)
	data, err := ioutil.ReadFile(ConfigLocation)
	if err != nil {
		log.Println("No config file in " + ConfigLocation)
		log.Println("If you want the config file to be here, please `touch " + ConfigLocation + "` so I know that's what you mean")
		log.Println("Otherwise, do --config-file /path/to/where/you/want/it/.gb.conf")
		os.Exit(1)
		return
	}
	if len(data) == 0 {
		log.Println("Empty config file. Filling in with defaults!")
		saveConfig()
		return
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Println("Error while loading config file!")
		panic(err)
	}
	sanity()
}

func sanity() {
	if config.PaddingMinBytes > config.PaddingMaxBytes {
		panic("PaddingMinBytes must be less than or equal to PaddingMaxBytes")
	}
	if config.PaddingMinPercent > config.PaddingMaxPercent {
		panic("PaddingMinPercent must be less than or equal to PaddingMaxPercent")
	}
	if config.NumHasherThreads < 1 {
		panic("NumHasherThreads must be at least 1")
	}
	if config.NumUploaderThreads < 1 {
		panic("NumUploaderThreads must be at least 1")
	}
	if config.UploadStatusInterval < -1 || config.UploadStatusInterval == 0 {
		panic("UploadStatusInterval must be -1 or positive")
	}
	mustBeLower(config.NoCompressionExts)
	mustBeLower(config.ExcludePrefixes)
	mustBeLower(config.ExcludeSuffixes)
	mustBeLower(config.DedupeExclude)
	mustEndWithSlash(config.Includes)
	if len(config.Includes) == 0 {
		panic("No include paths")
	}
	if config.RelayServer != "localhost" && !strings.HasPrefix(config.RelayServer, "192.168.") && !strings.HasPrefix(config.RelayServer, "127.0.0.") && !strings.HasPrefix(config.RelayServer, "10.") {
		panic("Relay is **NOT ENCRYPTED**. Refusing to relay to a non local IP. Do not relay over public internet. Use a ssh tunnel!")
	}

	dbAbs, err := filepath.Abs(config.DatabaseLocation)
	if err != nil {
		panic(err)
	}
	if config.DatabaseLocation != dbAbs {
		panic("DatabaseLocation must be absolute path")
	}
}

func mustBeLower(data []string) {
	for _, str := range data {
		if strings.ToLower(str) != str {
			panic(str + " must be lower case, to make it clear this is a case insensitive match")
		}
	}
}

func mustEndWithSlash(data []string) {
	for _, str := range data {
		if !strings.HasSuffix(str, "/") {
			panic(str + " in includes must end with a /")
		}
	}
}

func saveConfig() {
	data, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		panic(err) // impossible. marshal only errors on unrepresentatable datatypes like chan and func
	}
	err = ioutil.WriteFile(ConfigLocation, data, 0644)
	if err != nil {
		// possible
		log.Println("Error while writing config file!")
		panic(err)
	}
}

// rootPath is the path the scan was started from
func ExcludeFromBackup(rootPath string, path string) bool {
	path = strings.ToLower(path)
	rootPath = strings.ToLower(rootPath)
	for _, suffix := range config.ExcludeSuffixes {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	for _, prefix := range config.ExcludePrefixes {
		// if an exclude prefix is a parent of the path we are searching from we bypass the exclude
		if strings.HasPrefix(prefix, rootPath) && strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func ExcludeFromDedupe(path string) bool {
	path = strings.ToLower(path)
	for _, prefix := range config.DedupeExclude {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}
