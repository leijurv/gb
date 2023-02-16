package share

import (
	"crypto/tls"
	"log"
	"net/http"
	"strings"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/proxy"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
)

func Shared(label string, listen string) {
	storage, ok := storage.StorageSelect(label)
	if !ok {
		return
	}
	server := &http.Server{
		Addr: listen,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleHTTP(w, r, storage)
		}),
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)), // disables http/2
	}
	log.Println("Listening for HTTP on", listen)
	log.Fatal(server.ListenAndServe())
}

func handleHTTP(w http.ResponseWriter, req *http.Request, storage storage_base.Storage) {
	path := req.URL.Path
	if strings.HasPrefix(path, "/1/") {
		log.Println("Request to", path, "is presumably for a v1 shared file")
		hash, err := ValidateURL(path)
		if err != nil {
			w.WriteHeader(404)
			w.Write([]byte("sorry"))
			return
		}
		proxy.ServeHashOverHTTP(hash, w, req, storage)
		return
	}
	w.WriteHeader(404)
	w.Write([]byte("idk"))
}

var shareKey []byte

func SharingKey() []byte {
	if len(shareKey) == 0 {
		shareKey = crypto.ComputeMAC([]byte("sharing"), backup.DBKey())
	}
	if len(shareKey) != 32 {
		panic("bad key")
	}
	return shareKey
}
