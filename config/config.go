package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

var HomeDir = os.Getenv("HOME")
var ConfigLocation = HomeDir + "/.gb.conf"

type ConfigData struct {
	MinBlobSize        int64   `json:"min_blob_size"`
	DatabaseLocation   string  `json:"database_location"`
	PaddingMinBytes    int64   `json:"padding_min_bytes"`
	PaddingMaxBytes    int64   `json:"padding_max_bytes"`
	PaddingMinPercent  float64 `json:"padding_min_percent"`
	PaddingMaxPercent  float64 `json:"padding_max_percent"`
	NumHasherThreads   int     `json:"num_hasher_threads"`
	NumUploaderThreads int     `json:"num_uploader_threads"`
}

func Config() ConfigData {
	return config
}

var config = ConfigData{
	MinBlobSize:        64000000,
	DatabaseLocation:   HomeDir + "/.gb.db",
	PaddingMinBytes:    5021,
	PaddingMaxBytes:    12345,
	PaddingMinPercent:  0.05,
	PaddingMaxPercent:  0.1, // percent means percent. this is 0.1% not 10%!!
	NumHasherThreads:   4,
	NumUploaderThreads: 4,
}

func init() {
	log.Println("Assuming your home directory is " + HomeDir)
	log.Println("Therefore I'm going to assume my config file should be at " + ConfigLocation)
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
