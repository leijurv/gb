package paranoia

import (
	"database/sql"
	"encoding/hex"
	"io/fs"
	"log"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
)

// Querier is an interface that both *sql.DB and *sql.Tx implement
type Querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

var queriesThatShouldHaveNoRows = []string{
	// god i wish these could be database constraints :(
	"SELECT files.hash FROM files LEFT OUTER JOIN blob_entries ON files.hash = blob_entries.hash WHERE blob_entries.hash IS NULL",                                                                    // have a file, but it isn't backed up
	"SELECT blob_entries.hash FROM blob_entries LEFT OUTER JOIN files ON blob_entries.hash = files.hash WHERE files.hash IS NULL",                                                                    // backed something up for no reason
	"SELECT sizes.hash FROM sizes LEFT OUTER JOIN files ON sizes.hash = files.hash WHERE files.hash IS NULL",                                                                                         // know the size of a hash that doesn't exist
	"SELECT blobs.blob_id FROM blobs LEFT OUTER JOIN blob_entries ON blobs.blob_id = blob_entries.blob_id WHERE blob_entries.blob_id IS NULL",                                                        // know of a blob with no entries
	"SELECT blobs.blob_id FROM blobs LEFT OUTER JOIN blob_storage ON blobs.blob_id = blob_storage.blob_id WHERE blob_storage.blob_id IS NULL",                                                        // know of a blob that isn't stored anywhere
	"SELECT blobs.blob_id FROM blobs LEFT OUTER JOIN (SELECT * FROM blob_entries WHERE offset = 0) initial_entries ON blobs.blob_id = initial_entries.blob_id WHERE initial_entries.blob_id IS NULL", // know of a blob with no entry at offset 0

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

	// path sanity relating to slashes, ., .., etc
	`
	SELECT hash FROM files WHERE
		(path LIKE '%/') OR
		(path NOT LIKE '/%') OR
		(path LIKE '%//%') OR
		(path LIKE '%/./%') OR
		(path LIKE '%/.') OR
		(path LIKE '%/../%') OR
		(path LIKE '%/..') OR
		LENGTH(path) > 4096
	`,

	// future timestamps (60 second tolerance for clock skew between Go and SQLite, also helps with unit tests)
	"SELECT hash FROM files WHERE start > strftime('%s', 'now') + 60",
	"SELECT hash FROM files WHERE end > strftime('%s', 'now') + 60",
	"SELECT blob_id FROM blob_storage WHERE timestamp > strftime('%s', 'now') + 60",

	// prior to the gb epoch (first commit)
	"SELECT hash FROM files WHERE start < 1572924988",

	// uncompressed entries should have final_size = sizes.size
	"SELECT blob_entries.hash FROM blob_entries INNER JOIN sizes ON blob_entries.hash = sizes.hash WHERE blob_entries.compression_alg = '' AND blob_entries.final_size != sizes.size",

	// lepton should never make a file larger
	"SELECT blob_entries.hash FROM blob_entries INNER JOIN sizes ON blob_entries.hash = sizes.hash WHERE blob_entries.compression_alg = 'lepton' AND blob_entries.final_size > sizes.size",

	// currently understood storages
	`
	SELECT storage_id FROM storage WHERE type NOT IN (
		'S3',
		'GDrive',
		'Mock'
	)
	`,

	// currently understood compressions
	`
	SELECT blob_id FROM blob_entries WHERE compression_alg NOT IN (
		'',
		'zstd',
		'lepton'
	)
	`,

	// lepton is only used on jpgs
	`
	SELECT
		hash
	FROM
		blob_entries
	WHERE
		compression_alg = 'lepton'
		AND NOT EXISTS
			(
				SELECT
					1
				FROM
					files
				WHERE
					files.hash = blob_entries.hash
				AND (
					path LIKE '%.jpg' COLLATE NOCASE OR
					path LIKE '%.jpeg' COLLATE NOCASE
				)
			)
	`,

	// encryption keys should not be reused across different blobs
	"SELECT encryption_key FROM blob_entries GROUP BY encryption_key HAVING COUNT(DISTINCT blob_id) > 1",

	// permissions should be 0-511 (9 bits)
	"SELECT hash FROM files WHERE permissions < 0 OR permissions > 511",

	// duplicate final_hash in blobs (should be astronomically unlikely)
	"SELECT final_hash FROM blobs GROUP BY final_hash HAVING COUNT(*) > 1",

	// everything has been backed up to every destination
	"SELECT blob_id FROM blob_storage GROUP BY blob_id HAVING COUNT(*) != (SELECT COUNT(*) FROM storage) -- if this one fails it means that there is a blob that is in some storages but not all of them. `gb replicate` can help with this!",

	// nothing was ever backed up to the same place twice
	"SELECT blob_id FROM blob_storage GROUP BY blob_id, storage_id HAVING COUNT(*) > 1",

	// nothing was ever backed up twice
	"SELECT hash FROM blob_entries GROUP BY hash HAVING COUNT(*) > 1 -- if this one fails it means that you may have run two `gb backup` processes at once, and the same file got duplicated. you can fix this with `gb deduplicate`!",

	// checksum is de facto required
	"SELECT blob_id FROM blob_storage WHERE checksum IS NULL",

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

	// older blobs with 1 encryption key should not be shared
	"SELECT blob_id FROM blob_entries WHERE blob_id IN (SELECT blob_id FROM share_entries) GROUP BY blob_id HAVING COUNT(DISTINCT encryption_key) = 1 AND COUNT(*) > 1",

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
}

func DBParanoia() {
	DBParanoiaOn(db.DB)
}

func DBParanoiaTx(tx *sql.Tx) {
	DBParanoiaOn(tx)
}

func DBParanoiaOn(q Querier) {
	sqliteVerifyPragmaCheckOn(q, "quick_check")
	for _, query := range queriesThatShouldHaveNoRows {
		prettyQuery := strings.ReplaceAll(strings.ReplaceAll(query, "\n", ""), "\t", " ")
		start := time.Now()
		var result []byte
		err := q.QueryRow(query).Scan(&result)
		if err != db.ErrNoRows {
			panic("Failed database sanity on query `" + prettyQuery + "` " + hex.EncodeToString(result))
		}
		log.Println(prettyQuery, "took", time.Since(start))
	}
	sqliteVerifyForeignKeysOn(q)
	pathValidityOn(q)
	sqliteVerifyPragmaCheckOn(q, "integrity_check")
	blobsCoherenceOn(q)
	log.Println("Done running database paranoia")
}

func pathValidityOn(q Querier) {
	log.Println("Running files path validity check")
	rows, err := q.Query("SELECT path FROM files")
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
		if path[0] != '/' || !fs.ValidPath(path[1:]) {
			panic("invalid utf8 in the files database at path " + path)
		}
		cnt++
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Printf("Done running files path validity check on %d rows\n", cnt)
}

func sqliteVerifyPragmaCheckOn(q Querier, pragma string) {
	log.Println("Running sqlite `PRAGMA " + pragma + ";`")
	rows, err := q.Query("PRAGMA " + pragma)
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

func sqliteVerifyForeignKeysOn(q Querier) {
	log.Println("Running sqlite `PRAGMA foreign_key_check;`")
	rows, err := q.Query("PRAGMA foreign_key_check")
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

func blobsCoherenceOn(q Querier) {
	log.Println("Running blob entry coherence")
	rows, err := q.Query("SELECT blob_id, size FROM blobs")
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
		entriesCnt += blobCoherenceOn(q, blobID, size)
		cnt++
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Println("Verified entry coherence on", cnt, "blobs and", entriesCnt, "entries")
}

func blobCoherenceOn(q Querier, blobID []byte, size int64) int {
	rows, err := q.Query("SELECT final_size, offset FROM blob_entries WHERE blob_id = ? ORDER BY offset, final_size", blobID) // the ", final_size" serves to ensure that the empty entry comes before the nonempty entry at the same offset
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
