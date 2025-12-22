package paranoia

import (
	"encoding/hex"
	"log"
	"unicode/utf8"

	"github.com/leijurv/gb/config"
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

	// no longer possible with database layer 2 (the column is now NOT NULL)
	//"SELECT blob_id FROM blob_entries WHERE compression_alg IS NULL",

	"SELECT blob_id FROM blob_entries WHERE final_size = 0 AND compression_alg != ''",

	// ensure we only have one zero-byte blob entry
	"SELECT blob_id FROM blob_entries WHERE final_size = 0 AND rowid != (SELECT rowid FROM blob_entries WHERE final_size = 0 LIMIT 1)",
	// ensure we only have one zero-byte sizes entry
	"SELECT hash FROM sizes WHERE size = 0 AND rowid != (SELECT rowid FROM sizes WHERE size = 0 LIMIT 1)",

	// make sure that there are no two blob_entries at the same blob_id and offset, EXCEPT for the one entry that's allowed to be zero-byte
	`
	WITH collision AS (
		SELECT
			blob_id,
			offset,
			COUNT(*) AS cnt
		FROM
			blob_entries
		GROUP BY
			blob_id,
			offset
		HAVING
			cnt > 1
	),
	allowed_collision AS (
		SELECT
			blob_id,
			offset
		FROM
			blob_entries
		WHERE
			final_size = 0
	)
	SELECT
		blob_id
	FROM
		collision
	WHERE
		cnt != 2
		OR 
			(
				SELECT
					COUNT(*)
				FROM
					allowed_collision
				WHERE
					allowed_collision.blob_id = collision.blob_id
					AND allowed_collision.offset = collision.offset
			) = 0
	`,

	"SELECT hash FROM files WHERE path LIKE '%/'",
	"SELECT hash FROM files WHERE path NOT LIKE '/%'",

	// path sanity: double slashes
	"SELECT hash FROM files WHERE path LIKE '%//%'",

	// path sanity: . and .. components
	"SELECT hash FROM files WHERE path LIKE '%/./%' OR path LIKE '%/.'",
	"SELECT hash FROM files WHERE path LIKE '%/../%' OR path LIKE '%/..'",

	// future timestamps (60 second tolerance for clock skew between Go and SQLite)
	"SELECT hash FROM files WHERE start > strftime('%s', 'now') + 60",
	"SELECT hash FROM files WHERE end > strftime('%s', 'now') + 60",
	"SELECT blob_id FROM blob_storage WHERE timestamp > strftime('%s', 'now') + 60",

	// uncompressed entries should have final_size = sizes.size
	"SELECT blob_entries.hash FROM blob_entries INNER JOIN sizes ON blob_entries.hash = sizes.hash WHERE blob_entries.compression_alg = '' AND blob_entries.final_size != sizes.size",

	// zero-size files should have final_size = 0
	"SELECT blob_entries.hash FROM blob_entries INNER JOIN sizes ON blob_entries.hash = sizes.hash WHERE sizes.size = 0 AND blob_entries.final_size != 0",

	// encryption keys should not be reused across different blobs
	"SELECT encryption_key FROM blob_entries GROUP BY encryption_key HAVING COUNT(DISTINCT blob_id) > 1",

	// permissions should be 0-511 (9 bits)
	"SELECT hash FROM files WHERE permissions < 0 OR permissions > 511",

	// duplicate final_hash in blobs (should be astronomically unlikely)
	"SELECT final_hash FROM blobs GROUP BY final_hash HAVING COUNT(*) > 1",

	// same hash with same compression should produce same final_size (deterministic compression)
	"SELECT hash FROM blob_entries GROUP BY hash, compression_alg HAVING COUNT(DISTINCT final_size) > 1",

	// path length sanity (PATH_MAX is typically 4096)
	"SELECT hash FROM files WHERE LENGTH(path) > 4096",

	// everything has been backed up to every destination
	"SELECT blob_id FROM blob_storage GROUP BY blob_id HAVING COUNT(*) != (SELECT COUNT(*) FROM storage) -- if this one fails it means that there is a blob that is in some storages but not all of them. `gb replicate` can help with this!",

	// nothing was ever backed up to the same place twice
	"SELECT blob_id FROM blob_storage GROUP BY blob_id, storage_id HAVING COUNT(*) > 1",

	// nothing was ever backed up twice
	// "SELECT hash FROM blob_entries GROUP BY hash HAVING COUNT(*) > 1",
	// NEVER MIND this has happened a few times when another program was modifying files at the same time, such as creating two empty files that get backed up
	// it also happens if you run `gb backup` on the same folder in two different windows at the same time

	// if the same blob has been uploaded to two storages of the same type (such as S3), make sure that the path and checksum matches
	// this is a good sanity check after doing a `gb replicate`!
	`
	WITH all_stored AS (
		SELECT
			blob_id,
			storage_id,
			checksum,
			path,
			type
		FROM
			blob_storage
			INNER JOIN storage USING (storage_id)
	)
	SELECT
		blob_id
	FROM
		all_stored AS a
		INNER JOIN all_stored AS b USING (blob_id, type)
	WHERE
		a.storage_id < b.storage_id
		AND (a.path != b.path OR a.checksum != b.checksum)
	`,

	// blobs should have all the same encryption key for all entries (older blobs), or all different encryption keys (newer blobs)
	`
	WITH distinct_keys AS (
		SELECT
			blob_id,
			COUNT(DISTINCT encryption_key) AS cnt
		FROM
			blob_entries
		GROUP BY
			blob_id
	),
	entry_counts AS (
		SELECT
			blob_id,
			COUNT(*) AS cnt
		FROM
			blob_entries
		GROUP BY
			blob_id
	)
	SELECT
		blob_id
	FROM
		distinct_keys
		INNER JOIN entry_counts USING (blob_id)
	WHERE
		distinct_keys.cnt != 1
		AND distinct_keys.cnt != entry_counts.cnt
	`,

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
	sqliteVerifyPragmaCheck("quick_check")
	sqliteVerifyForeignKeys()
	for _, q := range queriesThatShouldHaveNoRows {
		log.Println("Running paranoia query:", q)
		var result []byte
		err := db.DB.QueryRow(q).Scan(&result)
		if err != db.ErrNoRows {
			log.Println(err)
			log.Println("Result is", hex.EncodeToString(result))
			panic("sanity query should have no rows")
		}
	}
	pathUtf8()
	sqliteVerifyPragmaCheck("integrity_check")
	blobsCoherence()
	log.Println("Done running database paranoia")
}

func pathUtf8() {
	log.Println("Running files path utf8 and control character check")
	rows, err := db.DB.Query("SELECT path FROM files")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	cnt := 0
	for rows.Next() {
		var path string
		err = rows.Scan(&path)
		if err != nil {
			panic(err)
		}
		if !utf8.ValidString(path) {
			panic("invalid utf8 in the files database at path " + path)
		}
		for _, r := range path {
			if r < 0x20 || r == 0x7F {
				panic("control character in path " + path)
			}
		}
		cnt++
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Printf("Done running files path utf8 and control character check on %d rows\n", cnt)
}

func sqliteVerifyPragmaCheck(pragma string) {
	log.Println("Running sqlite `PRAGMA " + pragma + ";`")
	rows, err := db.DB.Query("PRAGMA " + pragma)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	ok := false
	for rows.Next() {
		var result string
		err = rows.Scan(&result)
		if err != nil {
			panic(err)
		}
		if result != "ok" {
			panic("sqlite " + pragma + " failed " + result)
		}
		ok = true
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	if !ok {
		panic("`PRAGMA " + pragma + ";` returned no rows?")
	}
	log.Println("Done running sqlite `PRAGMA " + pragma + ";`")
}

func sqliteVerifyForeignKeys() {
	log.Println("Running sqlite `PRAGMA foreign_key_check;`")
	rows, err := db.DB.Query("PRAGMA foreign_key_check")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	failed := false
	for rows.Next() {
		var table string
		var rowid int64
		var referredTable string
		var foreignIdx int
		err = rows.Scan(&table, &rowid, &referredTable, &foreignIdx)
		if err != nil {
			panic(err)
		}
		log.Printf("Failed foreign key check: rowid %d in table `%s` wants to reference a matching row in table `%s` due to foreign key constraint index %d but there is none\n", rowid, table, referredTable, foreignIdx)
		failed = true
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	if failed {
		panic("`PRAGMA foreign_key_check;` failed, see above")
	}
	log.Println("Done running sqlite `PRAGMA foreign_key_check;`")
}

func blobsCoherence() {
	log.Println("Running blob entry coherence")
	rows, err := db.DB.Query("SELECT blob_id, size FROM blobs")
	if err != nil {
		panic(err)
	}
	cnt := 0
	entriesCnt := 0
	defer rows.Close()
	for rows.Next() {
		var blobID []byte
		var size int64
		err = rows.Scan(&blobID, &size)
		if err != nil {
			panic(err)
		}
		entriesCnt += blobCoherence(blobID, size)
		cnt++
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Println("Verified entry coherence on", cnt, "blobs and", entriesCnt, "entries")
}

func blobCoherence(blobID []byte, size int64) int {
	rows, err := db.DB.Query("SELECT final_size, offset FROM blob_entries WHERE blob_id = ? ORDER BY offset, final_size", blobID) // the ", final_size" serves to ensure that the empty entry comes before the nonempty entry at the same offset
	if err != nil {
		panic(err)
	}
	cnt := 0
	defer rows.Close()
	var nextStartsAt int64
	for rows.Next() {
		var finalSize int64
		var offset int64
		err = rows.Scan(&finalSize, &offset)
		if err != nil {
			panic(err)
		}
		cnt++
		if nextStartsAt != offset {
			log.Println(offset)
			log.Println(finalSize)
			log.Println(nextStartsAt)
			panic("incoherent blob_entries packing")
		}
		nextStartsAt += finalSize
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	remain := size - nextStartsAt
	if remain < config.Config().PaddingMinBytes+int64(float64(nextStartsAt)*(config.Config().PaddingMinPercent)/100) {
		panic("not enough padding at end of file")
	}
	if remain > config.Config().PaddingMaxBytes+int64(float64(nextStartsAt)*(config.Config().PaddingMaxPercent)/100) {
		panic("too much padding at end of file")
	}
	return cnt
}
