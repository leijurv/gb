package db

type DatabaseLayer int

const (
	DATABASE_LAYER_EMPTY = iota
	DATABASE_LAYER_1     // original schema, as of 2019
	DATABASE_LAYER_2     // hash_pre_enc removed, hash_post_enc renamed to final_hash, encryption_key renamed to padding_key, encryption_key added to blob_entries
	DATABASE_LAYER_3     // blob_entries_by_blob_id index replaced with unique blob_entries_by_blob_id_and_hash, unique index on blob_storage(blob_id, storage_id), shares table added
)

func initialSetup() {
	switch determineDatabaseLayer() {
	case DATABASE_LAYER_EMPTY:
		Must(schemaVersionOne())
		fallthrough
	case DATABASE_LAYER_1:
		Must(schemaVersionTwo())
		fallthrough
	case DATABASE_LAYER_2:
		Must(schemaVersionThree())
		fallthrough
	case DATABASE_LAYER_3:
		// up to date
	}
}

/*





Refer to schema.sql for the up-to-date schema of the gb database.

This file creates the schema "one layer at a time", starting with the original schema as it was in 2019, then applying updates on at a time.
For that reason, this file is less readable because of the incremental updates.
To see the final schema that gb actually uses today, refer to schema.sql





*/

func schemaVersionOne() error {
	tx, err := DB.Begin()
	Must(err)
	defer tx.Rollback()
	_, err = tx.Exec(`
	CREATE TABLE sizes (

		hash BLOB    NOT NULL PRIMARY KEY, /* sha256 of contents */
		size INTEGER NOT NULL,             /* length in bytes of the data that has this sha256 */

		CHECK(LENGTH(hash) == 32), /* sha256 length */
		CHECK(size >= 0) /* i am not making this mistake again. sadly google takeout has dozens of empty files as markers. we cannot assume size > 0 */
	);
	CREATE INDEX sizes_by_size ON sizes(size); /* this is used for the size check optimization */


	CREATE TABLE files (

		path        TEXT    NOT NULL, /* path on disk to the file */
		hash        BLOB    NOT NULL, /* sha256 of contents */
		start       INTEGER NOT NULL, /* timestamp of the first time this file existed with these contents (unix seconds) */
		end         INTEGER,          /* timestamp of when this file started not existing with these contents (unix seconds) */
		fs_modified INTEGER NOT NULL, /* a filesystem timestamp (unix seconds) */
		permissions INTEGER NOT NULL, /* the 9 least significant bits of the os stat filemode, describing the standard rwxrwxrwx permissions */

		UNIQUE(path, start), /* a path only appears once in a given backup */
		CHECK(LENGTH(path) > 1),
		CHECK(LENGTH(hash) == 32),
		CHECK(start > 0),
		CHECK(end IS NULL OR end > start),
		CHECK(fs_modified >= 0),
		CHECK(permissions >= 0),

		FOREIGN KEY(hash) REFERENCES sizes(hash) ON UPDATE RESTRICT ON DELETE RESTRICT
	);
	CREATE INDEX files_by_hash ON files(hash); /* needed when getting sources for a blob entry */
	CREATE INDEX files_by_path ON files(path); /* needed when getting the history of a file */
	CREATE UNIQUE INDEX files_by_path_and_end ON files(path, end) WHERE end IS NOT NULL; /* custom uniqueness constraint, ensures history is sane */
	CREATE UNIQUE INDEX files_by_path_curr ON files(path) WHERE end IS NULL; /* very important, allows efficient query of WHERE path=? AND end IS NULL, also requires that that query is unique in its result */


	CREATE TABLE blobs (

		blob_id        BLOB    NOT NULL PRIMARY KEY, /* random bytes */
		encryption_key BLOB    NOT NULL, /* random bytes */
		size           INTEGER NOT NULL, /* size in bytes. will be equal to padding + sum of entries sizes. size is the same pre and post encryption */
		hash_pre_enc   BLOB    NOT NULL, /* hash before encryption */
		hash_post_enc  BLOB    NOT NULL, /* hash after encryption */

		UNIQUE(encryption_key), /* paranoia */
		CHECK(LENGTH(blob_id) == 32),
		CHECK(LENGTH(encryption_key) == 16),
		CHECK(size > 0),
		CHECK(LENGTH(hash_pre_enc) == 32),
		CHECK(LENGTH(hash_post_enc) == 32)
	);


	CREATE TABLE blob_entries (

		hash            BLOB    NOT NULL, /* hash of what this is storing */
		blob_id         BLOB    NOT NULL, /* blob this is in */
		final_size      INTEGER NOT NULL, /* the length of this entry in bytes, i.e. size after compression, if any, has taken place */
		offset          INTEGER NOT NULL, /* where in the blob does this start */
		compression_alg TEXT,             /* what kind of compression was done */

/* it was a mistake to not add NOT NULL to compression_alg :( */
/* not the end of the world though */

		CHECK(final_size >= 0),
		CHECK(offset >= 0),

		FOREIGN KEY(hash)    REFERENCES sizes(hash)    ON UPDATE RESTRICT ON DELETE RESTRICT,
		FOREIGN KEY(blob_id) REFERENCES blobs(blob_id) ON UPDATE CASCADE  ON DELETE CASCADE
	);
	CREATE INDEX blob_entries_by_blob_id ON blob_entries(blob_id);
	CREATE INDEX blob_entries_by_hash    ON blob_entries(hash);


	CREATE TABLE storage (

		storage_id     BLOB NOT NULL PRIMARY KEY, /* identifier for this location in which we are  */
		readable_label TEXT NOT NULL, /* a label you can choose for this storage */
		type           TEXT NOT NULL, /* what kind of storage? e.g. disk, s3, gdrive */
		identifier     TEXT NOT NULL, /* any arbitrary data the storage needs to identify itself. could be a hard drive UUID, could be a s3 bucket name, could be an entire google drive oauth key, idk */
		root_path      TEXT NOT NULL, /* a folder that gb will save in */

		UNIQUE(readable_label),
		UNIQUE(type, identifier, root_path),
		CHECK(LENGTH(readable_label) > 0),
		CHECK(LENGTH(type) > 0),
		CHECK(LENGTH(identifier) > 0)
	);


	CREATE TABLE blob_storage (

		blob_id      BLOB    NOT NULL, /* blob this is storing, not unique since one blob can be backed up to multiple providers, thats allowed */
		storage_id   BLOB    NOT NULL, /* what is this being stored on */
		path         TEXT    NOT NULL, /* where in that is this. a path on s3, a file id on gdrive (for instant retrieval) */
		checksum     TEXT,             /* checksum in whatever format this provider uses (e.g. chunked md5 for s3, md5 for gdrive) */
		timestamp    INTEGER NOT NULL, /* when was this completed and inserted into the database (unix seconds) */

		UNIQUE(storage_id, path),
		CHECK(checksum IS NULL OR LENGTH(checksum) > 0), /* just a check stating that if you have no checksum, you should put in NULL, not an empty string */
		CHECK(LENGTH(path) > 0),
		CHECK(timestamp > 0),

		FOREIGN KEY(blob_id)    REFERENCES blobs(blob_id)      ON UPDATE CASCADE ON DELETE RESTRICT,
		FOREIGN KEY(storage_id) REFERENCES storage(storage_id) ON UPDATE CASCADE ON DELETE RESTRICT
	);
	CREATE INDEX blob_storage_by_blob_id ON blob_storage(blob_id);


	CREATE TABLE db_key (

		id  INTEGER NOT NULL PRIMARY KEY,
		key BLOB    NOT NULL,

		CHECK(id == 0), /* only one row allowed xD */
		CHECK(LENGTH(key) == 16)
	);
	`)
	if err != nil {
		return err
	}
	Must(tx.Commit())
	return nil
}

func schemaVersionTwo() error {
	_, err := DB.Exec("PRAGMA foreign_keys = OFF")
	Must(err)
	tx, err := DB.Begin()
	Must(err)
	// defer rollback BUT commit after successful execution
	defer tx.Rollback() // !!!intended to NOT actually rollback!!!
	_, err = tx.Exec(`
	CREATE TABLE blobs_temp (

		blob_id     BLOB    NOT NULL PRIMARY KEY, /* random bytes */
		padding_key BLOB    NOT NULL, /* random bytes, previously used to encrypt entire blob, now only used for the final padding bytes (for verification and reproducibility of blob creation) */
		size        INTEGER NOT NULL, /* size in bytes. will be equal to padding + sum of entries sizes. size is the same pre and post encryption */
		final_hash  BLOB    NOT NULL, /* hash after encryption */

		UNIQUE(padding_key), /* paranoia */
		CHECK(LENGTH(blob_id) == 32),
		CHECK(LENGTH(padding_key) == 16),
		CHECK(size > 0),
		CHECK(LENGTH(final_hash) == 32)
	);

	CREATE TABLE blob_entries_temp (

		hash            BLOB    NOT NULL, /* hash of what this is storing */
		blob_id         BLOB    NOT NULL, /* blob this is in */
		encryption_key  BLOB    NOT NULL, /* random bytes. old blobs will have the same key for each entry (compatibility); new blobs will have different keys for each entry */
		final_size      INTEGER NOT NULL, /* the length of this entry in bytes, i.e. size after compression, if any, has taken place */
		offset          INTEGER NOT NULL, /* where in the blob does this start. also, for compatibility reasons, where in the AES CTR stream does this entry's encryption begin */
		compression_alg TEXT    NOT NULL, /* what kind of compression was done (empty string if not compressed) */

		CHECK(final_size >= 0),
		CHECK(offset >= 0),
		CHECK(LENGTH(encryption_key) == 16),

		FOREIGN KEY(hash)    REFERENCES sizes(hash)    ON UPDATE RESTRICT ON DELETE RESTRICT,
		FOREIGN KEY(blob_id) REFERENCES blobs(blob_id) ON UPDATE CASCADE  ON DELETE CASCADE
	);

	INSERT INTO blobs_temp(blob_id, padding_key, size, final_hash) SELECT blob_id, encryption_key, size, hash_post_enc FROM blobs;

	INSERT INTO blob_entries_temp(hash, blob_id, encryption_key, final_size, offset, compression_alg) SELECT hash, blob_id, blobs.encryption_key, final_size, offset, compression_alg FROM blob_entries INNER JOIN blobs USING (blob_id);

	DROP INDEX blob_entries_by_blob_id;
	DROP INDEX blob_entries_by_hash;
	DROP TABLE blobs;
	DROP TABLE blob_entries;

	ALTER TABLE blobs_temp RENAME TO blobs;
	ALTER TABLE blob_entries_temp RENAME TO blob_entries;

	CREATE INDEX blob_entries_by_blob_id ON blob_entries(blob_id);
	CREATE INDEX blob_entries_by_hash    ON blob_entries(hash);
	`)
	if err != nil {
		return err
	}
	Must(tx.Commit())
	_, err = DB.Exec("PRAGMA foreign_keys = ON")
	Must(err)
	return nil
}

func schemaVersionThree() error {
	tx, err := DB.Begin()
	Must(err)
	defer tx.Rollback()
	_, err = tx.Exec(`
	DROP INDEX blob_entries_by_blob_id;
	DROP INDEX blob_storage_by_blob_id;
	CREATE UNIQUE INDEX blob_entries_by_blob_id_and_hash ON blob_entries(blob_id, hash);
	CREATE UNIQUE INDEX blob_storage_by_blob_id_and_storage_id ON blob_storage(blob_id, storage_id);

	CREATE TABLE shares (

		password   TEXT    NOT NULL PRIMARY KEY, /* the password (not including the URL or the ".json") */
		name       TEXT    NOT NULL, /* the name of the share as a whole (e.g. "photos.zip") */
		storage_id BLOB    NOT NULL, /* which storage was the share JSON uploaded to? */
		shared_at  INTEGER NOT NULL, /* when was it shared (unix seconds) */
		expires_at INTEGER,          /* when does it expire (unix seconds) */
		revoked_at INTEGER,          /* when did we revoke this share (unix seconds) */

		UNIQUE(password, storage_id), /* needed for share_entries foreign key */
		CHECK(LENGTH(password) > 0),
		CHECK(LENGTH(name) > 0),
		CHECK(shared_at > 0),
		CHECK(expires_at IS NULL OR expires_at > shared_at),
		CHECK(revoked_at IS NULL OR revoked_at > shared_at),

		FOREIGN KEY(storage_id) REFERENCES storage(storage_id) ON UPDATE CASCADE ON DELETE RESTRICT
	);
	CREATE INDEX shares_by_shared_at ON shares(shared_at);

	CREATE TABLE share_entries (

		password   TEXT    NOT NULL, /* the password (not including the URL or the ".json") */
		hash       BLOB    NOT NULL, /* which hash was shared */
		filename   TEXT    NOT NULL, /* what name was it given? usually the same as the original filename (not including folder name) */
		blob_id    BLOB    NOT NULL, /* in which specific blob did we promise this hash could be found? */
		storage_id BLOB    NOT NULL, /* which storage was the share JSON uploaded to? */
		ordinal    INTEGER NOT NULL, /* order of entry in the share (0-indexed), determines order in zip */

		UNIQUE(password, filename),
		UNIQUE(password, ordinal),
		CHECK(LENGTH(filename) > 0),
		CHECK(ordinal >= 0),

		FOREIGN KEY(password, storage_id) REFERENCES shares(password, storage_id)      ON UPDATE CASCADE  ON DELETE CASCADE,
		FOREIGN KEY(blob_id, hash)        REFERENCES blob_entries(blob_id, hash)       ON UPDATE RESTRICT ON DELETE RESTRICT,
		FOREIGN KEY(blob_id, storage_id)  REFERENCES blob_storage(blob_id, storage_id) ON UPDATE CASCADE  ON DELETE RESTRICT
	);
	CREATE INDEX share_entries_by_hash ON share_entries(hash);
	`)
	if err != nil {
		return err
	}
	Must(tx.Commit())
	return nil
}

func query(query string) string {
	rows, err := DB.Query(query)
	Must(err)
	defer rows.Close()
	ret := ""
	for rows.Next() {
		var tableName string
		Must(rows.Scan(&tableName))
		ret = ret + tableName + ","
	}
	Must(rows.Err())
	return ret
}

func determineDatabaseLayer() DatabaseLayer {
	tables := query("SELECT name FROM sqlite_master WHERE type = 'table' AND name != 'sqlite_stat1' ORDER BY name")
	if tables == "" {
		return DATABASE_LAYER_EMPTY
	}

	// determine layer by tables
	expectedTablesLayer2 := "blob_entries,blob_storage,blobs,db_key,files,sizes,storage,"
	expectedTablesLayer3 := "blob_entries,blob_storage,blobs,db_key,files,share_entries,shares,sizes,storage,"
	isLayer3Tables := tables == expectedTablesLayer3
	if tables != expectedTablesLayer2 && !isLayer3Tables {
		panic("gb.db doesn't have the tables that I expect. expected '" + expectedTablesLayer2 + "' or '" + expectedTablesLayer3 + "' but got '" + tables + "'")
	}

	// check indexes match the layer determined by tables
	indexes := query("SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name")
	expectedIndexesLayer2 := "blob_entries_by_blob_id,blob_entries_by_hash,blob_storage_by_blob_id,files_by_hash,files_by_path,files_by_path_and_end,files_by_path_curr,sizes_by_size,sqlite_autoindex_blob_storage_1,sqlite_autoindex_blobs_1,sqlite_autoindex_blobs_2,sqlite_autoindex_files_1,sqlite_autoindex_sizes_1,sqlite_autoindex_storage_1,sqlite_autoindex_storage_2,sqlite_autoindex_storage_3,"
	expectedIndexesLayer3 := "blob_entries_by_blob_id_and_hash,blob_entries_by_hash,blob_storage_by_blob_id_and_storage_id,files_by_hash,files_by_path,files_by_path_and_end,files_by_path_curr,share_entries_by_hash,shares_by_shared_at,sizes_by_size,sqlite_autoindex_blob_storage_1,sqlite_autoindex_blobs_1,sqlite_autoindex_blobs_2,sqlite_autoindex_files_1,sqlite_autoindex_share_entries_1,sqlite_autoindex_share_entries_2,sqlite_autoindex_shares_1,sqlite_autoindex_shares_2,sqlite_autoindex_sizes_1,sqlite_autoindex_storage_1,sqlite_autoindex_storage_2,sqlite_autoindex_storage_3,"
	if isLayer3Tables {
		if indexes != expectedIndexesLayer3 {
			panic("gb.db has layer 3 tables but indexes don't match. expected '" + expectedIndexesLayer3 + "' but got '" + indexes + "'")
		}
	} else {
		if indexes != expectedIndexesLayer2 {
			panic("gb.db has layer 2 tables but indexes don't match. expected '" + expectedIndexesLayer2 + "' but got '" + indexes + "'")
		}
	}

	// distinguish layer 1 from layer 2 by blob columns
	blob_cols := query("SELECT name FROM PRAGMA_TABLE_INFO('blobs')")
	if blob_cols == "blob_id,encryption_key,size,hash_pre_enc,hash_post_enc," {
		return DATABASE_LAYER_1
	}
	expectedBlobCols := "blob_id,padding_key,size,final_hash,"
	if blob_cols != expectedBlobCols {
		panic("the 'blobs' table doesn't have the columns that I expect. expected '" + expectedBlobCols + "' but got '" + blob_cols + "'")
	}
	if isLayer3Tables {
		return DATABASE_LAYER_3
	}
	return DATABASE_LAYER_2
}
