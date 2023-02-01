package db

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/leijurv/gb/utils"
)

func WithTestingDatabase(t *testing.T, fn func()) {
	SetupDatabaseTestMode()
	defer ShutdownDatabase()
	fn()
}

func TestInitialSetup(t *testing.T) {
	WithTestingDatabase(t, func() {
		var i int64
		err := DB.QueryRow("SELECT 1+1").Scan(&i)
		if err != nil {
			t.Error(err)
		}
		if i != 2 {
			t.Errorf("1+1 != 2")
		}
	})
}

func TestConstraints(t *testing.T) {
	WithTestingDatabase(t, func() {
		_, err := DB.Exec("INSERT INTO sizes (hash, size) VALUES (?, ?)", make([]byte, 5), 0)
		if err == nil {
			t.Errorf("should not be allowed ")
		}
	})
}

func TestBlobFetch(t *testing.T) {
	WithTestingDatabase(t, func() {
		meme := sha256.Sum256([]byte("meme"))
		_, err := DB.Exec("INSERT INTO sizes (hash, size) VALUES (?, ?)", meme[:], 5021)
		if err != nil {
			t.Error(err)
		}
		var resp []byte
		err = DB.QueryRow("SELECT hash FROM sizes WHERE size = ?", 5021).Scan(&resp)
		if err != nil {
			t.Error(err)
		}
		for i := range resp {
			if resp[i] != meme[i] {
				t.Errorf("wrong")
			}
		}
	})
}

func TestGlob(t *testing.T) {
	WithTestingDatabase(t, func() {
		for _, pattern := range []string{"meow", "a", "a[", "[a", "a]", "]a", "a[b", "a]b", "a[b]", "[a]b", "a]b[", "]a[b", "]a]b", "a]b]", "[a[b", "a[b[", "a[b]c", "[][]][][]]]][[[]", "][[]][[]][][][][[]][][[]]["} {
			if strings.Contains(pattern, "[") && globs(t, pattern, pattern) {
				t.Errorf(pattern + " shouldn't glob itself on its own, since it has a [")
			}
			if !globs(t, pattern, utils.FormatForSqliteGlob(pattern)) {
				t.Errorf(pattern + " should glob itself when converted to " + utils.FormatForSqliteGlob(pattern))
			}
		}
	})
}

func globs(t *testing.T, test string, pattern string) bool {
	var ret bool
	err := DB.QueryRow("SELECT ? GLOB ?", test, pattern).Scan(&ret)
	if err != nil {
		t.Error(err)
	}
	return ret
}
