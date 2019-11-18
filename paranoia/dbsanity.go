package paranoia

import (
	"log"

	"github.com/leijurv/gb/db"
)

var queriesThatShouldHaveNoRows = []string{
	// god i wish these could be database constraints :(
	"SELECT files.hash FROM files LEFT OUTER JOIN blob_entries ON files.hash = blob_entries.hash WHERE blob_entries.hash IS NULL",                                                                    // have a file, but it isn't backed up
	"SELECT blob_entries.hash FROM blob_entries LEFT OUTER JOIN files ON blob_entries.hash = files.hash WHERE files.hash IS NULL",                                                                    // backed something up for no reason
	"SELECT sizes.hash FROM sizes LEFT OUTER JOIN files ON sizes.hash = files.hash WHERE files.hash IS NULL",                                                                                         // know the size of a hash that doesn't exist
	"SELECT blobs.blob_id FROM blobs LEFT OUTER JOIN blob_entries ON blobs.blob_id = blob_entries.blob_id WHERE blob_entries.blob_id IS NULL",                                                        // know of a blob with no entries
	"SELECT blobs.blob_id FROM blobs LEFT OUTER JOIN blob_storage ON blobs.blob_id = blob_storage.blob_id WHERE blob_storage.blob_id IS NULL",                                                        // know of a blob that isn't stored anywhere
	"SELECT blobs.blob_id FROM blobs LEFT OUTER JOIN (SELECT * FROM blob_entries WHERE offset = 0) initial_entries ON blobs.blob_id = initial_entries.blob_id WHERE initial_entries.blob_id IS NULL", // know of a blob with no entry at offset 0

	"SELECT blob_id FROM blob_entries WHERE compression_alg IS NULL", // i really should have made this a NOT NULL. my mistake.
}

func DBParanoia() {
	for _, q := range queriesThatShouldHaveNoRows {
		log.Println("Running paranoia query:", q)
		var result []byte
		err := db.DB.QueryRow(q).Scan(&result)
		if err != db.ErrNoRows {
			log.Println(err)
			panic("sanity query should have no rows")
		}
	}
	log.Println("Done running database paranoia")
}
