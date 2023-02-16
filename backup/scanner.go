package backup

import (
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

// use the includePaths that are deeper than the inputPath but if there are none just use the inputPath
func getDirectoriesToScan(inputPath string, includePaths []string) []string {
	out := make([]string, 0)
	for _, include := range includePaths {
		if strings.HasPrefix(include, inputPath) {
			out = append(out, include)
		}
	}
	if len(out) > 0 {
		return out
	} else {
		return []string{inputPath}
	}
}

func scannerThread(inputs []File) {
	var ctx ScannerTransactionContext
	log.Println("Beginning scan now!")
	for _, input := range inputs {
		if input.info.IsDir() {
			filesMap := make(map[string]os.FileInfo)
			for _, exclude := range config.Config().ExcludePrefixes {
				if strings.HasPrefix(input.path, exclude) {
					log.Printf("Input input bypasses exclude \"%s\"\n", exclude)
					// maybe add a sleep here to be safe?
				}
			}
			pathsToBackup := getDirectoriesToScan(input.path, config.Config().Includes)
			for _, path := range pathsToBackup {
				utils.WalkFiles(path, func(path string, info os.FileInfo) {
					filesMap[path] = info
					scanFile(File{path, info}, ctx.Tx())
				})
			}
			defer func() {
				for _, path := range pathsToBackup {
					pruneDeletedFiles(path, filesMap)
				}
			}()
		} else {
			scanFile(input, ctx.Tx())
		}
	}
	log.Println("Scanner committing")
	ctx.Close() // do this before wg.Wait
	log.Println("Scanner committed")
	go func() {
		for {
			time.Sleep(1 * time.Second)
			bucketerCh <- Planned{}
		}
	}()
	close(hasherCh)
	wg.Wait()
}

func scanFile(file File, tx *sql.Tx) {
	status := CompareFileToDb(file.path, file.info, tx, true)
	if !status.Modified && !status.New {
		return
	}

	// check if there is an existing file of this length
	size := status.size
	var otherHash []byte
	err := tx.QueryRow("SELECT hash FROM sizes WHERE size = ?", size).Scan(&otherHash)
	if err != nil {
		if err != db.ErrNoRows {
			panic(err) // unexpected error, maybe sql syntax error?
		}
		// ErrNoRows = no existing files of this size stored in the db! we can do the bypass!
		if stakeSizeClaim(size) { // no files of this size in the database yet... but check if there is already one in progress Right Now?
			// UwU we CAN do the bypass YAY
			log.Println("Staked size claim", size, "skipping hasher directly to bucketer epic style", file.path)
			wg.Add(1)
			bucketerCh <- Planned{file, nil, nil, &size}
			log.Println("wrote", file.path)
			return
		}
	}
	// no bypass :(
	// we know of a file with the exact same size (either in db, or currently being uploaded)
	// so we do actually need to check the hash of this file to determine if it's unique or not
	hasherCh <- HashPlan{file, nil}
}

// find files in the database for this path, that no longer exist on disk (i.e. they're DELETED LOL)
func pruneDeletedFiles(backupPath string, filesMap map[string]os.FileInfo) {
	// we cannot upgrade the long lived RO transaction to a RW transaction, it would conflict with the intermediary RW transactions, it seems
	// reusing the tx from scanner results in a sqlite busy panic, very consistently
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	if !strings.HasSuffix(backupPath, "/") {
		panic(backupPath) // sanity check, should have already been completed
	}
	log.Println("Finally, handling deleted files!")
	// anything that was in this directory but is no longer can be deleted
	rows, err := tx.Query("SELECT path FROM files WHERE end IS NULL AND path "+db.StartsWithPattern(1), backupPath)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var databasePath string
		err := rows.Scan(&databasePath)
		if err != nil {
			panic(err)
		}
		if _, ok := filesMap[databasePath]; !ok {
			log.Println(databasePath, "used to exist but does not any longer. Marking as ended.")
			_, err = tx.Exec("UPDATE files SET end = ? WHERE path = ? AND end IS NULL", now, databasePath)
			if err != nil {
				panic(err)
			}
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Println("Pruner committing")
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	log.Println("Pruner committed")
}

type ScannerTransactionContext struct {
	tx             *sql.Tx
	recreateTicker *time.Ticker
}

// leaving a single transaction open for the entire backup process causes the WAL to grow unboundedly
// see issue #31
// this fixes that issue by committing and reopening the transaction once a second
func (ctx *ScannerTransactionContext) Tx() *sql.Tx {
	if ctx.recreateTicker == nil {
		ctx.recreateTicker = time.NewTicker(1 * time.Second)
	}
	if ctx.tx == nil {
		tx, err := db.DB.Begin()
		if err != nil {
			panic(err)
		}
		ctx.tx = tx
		return tx
	}
	select {
	case <-ctx.recreateTicker.C:
		log.Println("Committing and recreating scanner transaction to prevent WAL from growing too large")
		ctx.Close()
		return ctx.Tx()
	default:
		return ctx.tx
	}
}

func (ctx *ScannerTransactionContext) Close() {
	if ctx.tx != nil {
		err := ctx.tx.Commit()
		if err != nil {
			panic(err)
		}
		ctx.tx = nil
	}
}
