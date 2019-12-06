package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var HomeDir = os.Getenv("HOME")
var ConfigLocation string
var inited = false

type ConfigData struct {
	MinBlobSize        int64    `json:"min_blob_size"`
	DatabaseLocation   string   `json:"database_location"`
	PaddingMinBytes    int64    `json:"padding_min_bytes"`
	PaddingMaxBytes    int64    `json:"padding_max_bytes"`
	PaddingMinPercent  float64  `json:"padding_min_percent"`
	PaddingMaxPercent  float64  `json:"padding_max_percent"`
	NumHasherThreads   int      `json:"num_hasher_threads"`
	NumUploaderThreads int      `json:"num_uploader_threads"`
	NoCompressionExts  []string `json:"no_compression_exts"`
	ExcludeSuffixes    []string `json:"exclude_suffixes"`
	ExcludePrefixes    []string `json:"exclude_prefixes"`
	DedupeExclude      []string `json:"dedupe_exclude"`
}

func Config() ConfigData {
	begin()
	return config
}

var config = ConfigData{
	MinBlobSize:        64000000,
	DatabaseLocation:   HomeDir + "/.gb.db",
	PaddingMinBytes:    5021,
	PaddingMaxBytes:    12345,
	PaddingMinPercent:  0.05,
	PaddingMaxPercent:  0.1, // percent means percent. this is 0.1% not 10%!!
	NumHasherThreads:   2,
	NumUploaderThreads: 8,
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
		"aes",
		"gpg",
		"aac",
		"opus",
		"ogg",
		"wmv",
		"rar",
		"dmg",
	},
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
	mustBeLower(config.NoCompressionExts)
	mustBeLower(config.ExcludePrefixes)
	mustBeLower(config.ExcludeSuffixes)
	mustBeLower(config.DedupeExclude)
}

func mustBeLower(data []string) {
	for _, str := range data {
		if strings.ToLower(str) != str {
			panic(str + " must be lower case, to make it clear this is a case insensitive match")
		}
	}
}

func saveConfig() {
	data, err := json.Marshal(config)
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

func ExcludeFromBackup(path string) bool {
	path = strings.ToLower(path)
	for _, suffix := range config.ExcludeSuffixes {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	for _, prefix := range config.ExcludePrefixes {
		if strings.HasPrefix(path, prefix) {
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
