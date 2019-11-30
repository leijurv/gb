package db

import (
	"log"
)

func initialSetup() {
	err := createTables()
	if err != nil {
		panic(err)
	}
}

func createTables() error {
	tx, err := DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	}()
	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS sizes (

		hash BLOB    NOT NULL PRIMARY KEY, /* sha256 of contents */
		size INTEGER NOT NULL,             /* timestamp of the first time this file existed with these contents */

		CHECK(LENGTH(hash) == 32), /* sha256 length */
		CHECK(size >= 0) /* i am not making this mistake again. sadly google takeout has dozens of empty files as markers. we cannot assume size > 0 */
	);
	CREATE INDEX IF NOT EXISTS sizes_by_size ON sizes(size); /* this is used for the size check optimization */
	`)
	if err != nil {
		log.Println("Unable to create sizes table")
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS files (

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
	CREATE INDEX IF NOT EXISTS files_by_hash ON files(hash); /* needed when getting sources for a blob entry */
	CREATE INDEX IF NOT EXISTS files_by_path ON files(path); /* needed when getting the history of a file */
	CREATE UNIQUE INDEX IF NOT EXISTS files_by_path_and_end ON files(path, end) WHERE end IS NOT NULL; /* custom uniqueness constraint, ensures history is sane */
	CREATE UNIQUE INDEX IF NOT EXISTS files_by_path_curr ON files(path) WHERE end IS NULL; /* very important, allows efficient query of WHERE path=? AND end IS NULL, also requires that that query is unique in its result */
	`)
	if err != nil {
		log.Println("Unable to create files table")
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS blobs (

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
	`)
	if err != nil {
		log.Println("Unable to create blobs table")
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS blob_entries (

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
	CREATE INDEX IF NOT EXISTS blob_entries_by_blob_id ON blob_entries(blob_id);
	CREATE INDEX IF NOT EXISTS blob_entries_by_hash    ON blob_entries(hash);
	`)
	if err != nil {
		log.Println("Unable to create blob_entries table")
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS storage (

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
	`)
	if err != nil {
		log.Println("Unable to create storage table")
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS blob_storage (

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
	CREATE INDEX IF NOT EXISTS blob_storage_by_blob_id ON blob_storage(blob_id);
	`)
	if err != nil {
		log.Println("Unable to create blob_storage table")
		return err
	}

	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS db_key (

		id  INTEGER NOT NULL PRIMARY KEY,
		key BLOB    NOT NULL,

		CHECK(id == 0), /* only one row allowed xD */
		CHECK(LENGTH(key) == 16)
	);
	`)
	if err != nil {
		log.Println("Unable to create db_key table")
		return err
	}

	return nil
}
