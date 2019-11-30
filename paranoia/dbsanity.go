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

	"SELECT blob_id FROM blob_entries WHERE final_size = 0 AND compression_alg != ''",

	// these next two could totally be rewritten as one query with a WHERE giant_condition_1 OR giant_condition_2
	// but it's super slow since it can't efficiently use indexes then
	// these two are SUPER fast as-is, no need to combine

	// find overlaps in files
	// find two rows, representing the same path, where the range of row1 (start to end) contains the start of row2
	`
	SELECT 
		files1.hash
	FROM files files1
		INNER JOIN files files2 ON files1.path = files2.path
	WHERE
		files1.end IS NOT NULL /* checking if files2's start is in is files1's start to end range, this works because of the unique partial index on path on and where end is not null */
		AND files2.start > files1.start /* given the UNIQUE(path, start) this is how to dedupe rows (it's not >=) */
		AND files2.start < files1.end
		/* files2.end can be null or not null, we don't know */
	`,

	// make sure that the entry in files with no end is the last one, when ordered by start
	// find two rows, representing the same path, where row1 has no end but is before (and therefore implicitly contains) the start of row2
	`
	SELECT 
		files1.hash
	FROM files files1
		INNER JOIN files files2 ON files1.path = files2.path
	WHERE
		files1.end IS NULL /* checking if files2's start is in is files1's start to end range, this works because of the unique partial index on path where end is null */
		AND files2.end IS NOT NULL /* this is an optimization that sqlite can't figure out. the unique partial index on path where end is null implies that if row1.end is null then row2.end can't also be null since they're the same path. but sqlite can't figure this out sadly */
		AND files2.start > files1.start /* given the UNIQUE(path, start) this is how to dedupe rows (it's not >=) */
	`,

	// these are already foreign key constraints, but some enterprising user who manually touches the database might screw em up
	"SELECT files.hash FROM files LEFT OUTER JOIN sizes ON files.hash = sizes.hash WHERE sizes.hash IS NULL",
	"SELECT blob_entries.hash FROM blob_entries LEFT OUTER JOIN sizes ON blob_entries.hash = sizes.hash WHERE sizes.hash IS NULL",
	"SELECT blob_entries.blob_id FROM blob_entries LEFT OUTER JOIN blobs ON blob_entries.blob_id = blobs.blob_id WHERE blobs.blob_id IS NULL",
	"SELECT blob_storage.blob_id FROM blob_storage LEFT OUTER JOIN blobs ON blob_storage.blob_id = blobs.blob_id WHERE blobs.blob_id IS NULL",
	"SELECT blob_storage.storage_id FROM blob_storage LEFT OUTER JOIN storage ON blob_storage.storage_id = storage.storage_id WHERE storage.storage_id IS NULL",
	// don't verify check constraints or unique constraints.
	// reason: foreign keys are pragma disabled by default, so it's possible to unknowingly violate them
	// not the case for check constraints / uniques. you can't accidentally violate those, you have to explicitly disable or remove them
	// if you do that, then your funeral
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
