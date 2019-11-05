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
	MinBlobSize      int64  `json:"min_blob_size"`
	DatabaseLocation string `json:"database_location"`
}

func Config() ConfigData {
	return config
}

var config = ConfigData{
	MinBlobSize:      16000000,
	DatabaseLocation: HomeDir + "/.gb.db",
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
