package proxy

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/leijurv/gb/cache"
	"github.com/leijurv/gb/compression"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/storage_base"
)

func ServeHashOverHTTP(hash []byte, w http.ResponseWriter, req *http.Request, storage storage_base.Storage) {
	_, clientHasRange := req.Header["Range"]
	var realContentLength int64
	err := db.DB.QueryRow("SELECT size FROM sizes WHERE hash = ?", hash).Scan(&realContentLength)
	db.Must(err)
	var blobID []byte
	var path string
	var key []byte
	var compressedSize int64
	var offsetIntoBlob int64
	var comp string
	err = db.DB.QueryRow(
		"SELECT blob_entries.blob_id, blob_entries.encryption_key, blob_storage.path, blob_entries.final_size, blob_entries.offset, blob_entries.compression_alg FROM blob_entries INNER JOIN blob_storage ON blob_storage.blob_id = blob_entries.blob_id INNER JOIN blobs ON blobs.blob_id = blob_storage.blob_id WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?",
		hash, storage.GetID()).Scan(&blobID, &key, &path, &compressedSize, &offsetIntoBlob, &comp)
	db.Must(err)
	log.Println(req)
	log.Println("Offset into blob", offsetIntoBlob)
	claimedLength := compressedSize
	// ^ seems like a bit of a footgun, but since Range header isn't supported with compression, it doesn't cause any issues?
	// for compressed files, it needs to be this way for storoage.DownloadSection and crypto.DecryptBlobEntry, but for Range queries it's user-defined
	seekStart := offsetIntoBlob
	var requestedStart int64
	respondWithRange := false
	if clientHasRange {
		if comp != "" {
			http.Error(w, "this blob entry is compressed, random seeking is not currently supported for compression sorry", http.StatusServiceUnavailable)
			return
		}

		r := req.Header["Range"][0]
		log.Println("Range requested", r)
		r = strings.Split(r, "bytes=")[1]
		lower := strings.Split(r, "-")[0]
		upper := strings.Split(r, "-")[1]
		requestedStart, err = strconv.ParseInt(lower, 10, 64)
		seekStart += requestedStart
		if err != nil {
			panic(err)
		}
		if upper == "" {
			claimedLength = realContentLength - requestedStart
			req.Header.Set("Range", "bytes="+strconv.FormatInt(seekStart, 10)+"-")
		} else {
			upperP, err := strconv.ParseInt(upper, 10, 64)
			if err != nil {
				panic(err)
			}
			claimedLength = upperP - requestedStart + 1
			req.Header.Set("Range", "bytes="+strconv.FormatInt(seekStart, 10)+"-"+strconv.FormatInt(seekStart+claimedLength-1, 10))
		}
		log.Println("Updated range to", req.Header["Range"][0])
		respondWithRange = true
	} else {
		if offsetIntoBlob != 0 {
			req.Header.Set("Range", "bytes="+strconv.FormatInt(seekStart, 10)+"-"+strconv.FormatInt(seekStart+claimedLength-1, 10))
		}
	}
	fullRead := !clientHasRange || (requestedStart == 0 && claimedLength == realContentLength)

	var data io.ReadCloser
	if path[:3] == "gb/" && os.Getenv("GB_HTTP_PROXY_PATTERN") != "" {
		pattern := os.Getenv("GB_HTTP_PROXY_PATTERN")
		log.Println("HTTP proxy pattern", pattern)
		pattern = strings.Replace(pattern, "#", path, -1)
		log.Println("Replaced", pattern)
		target, err := url.Parse(pattern)
		if err != nil {
			panic(err)
		}
		req.URL = target
		req.Host = target.Host
		log.Println(req)
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		data = resp.Body
	} else {
		data = cache.DownloadSection(storage, path, seekStart, claimedLength)
		//data = storage.DownloadSection(path, seekStart, claimedLength)

	}
	defer data.Close()

	decrypted := crypto.DecryptBlobEntry(io.LimitReader(data, claimedLength), seekStart, key)
	reader := compression.ByAlgName(comp).Decompress(decrypted)
	if fullRead {
		reader = download.WrapWithHashVerification(reader, hash, realContentLength)
	}
	writeHttpResponse(w, reader, requestedStart, claimedLength, realContentLength, req.URL.Path, respondWithRange)
}

func writeHttpResponse(w http.ResponseWriter, reader io.ReadCloser, start int64, claimedLength int64, realLength int64, path string, respondWithRange bool) {
	h := w.Header()
	// for everything else let the http library figure out the content type
	if strings.HasSuffix(strings.ToLower(path), ".mp4") {
		h.Add("Content-Type", "video/mp4")
	} else if strings.HasSuffix(strings.ToLower(path), ".mkv") {
		h.Add("Content-Type", "video/x-matroska")
	} else if strings.HasSuffix(strings.ToLower(path), ".png") {
		h.Add("Content-Type", "image/png")
	} else if strings.HasSuffix(strings.ToLower(path), ".jpg") {
		h.Add("Content-Type", "image/jpeg")
	}
	h.Add("Connection", "keep-alive")
	h.Add("Accept-Ranges", "bytes")
	h.Add("Content-Length", strconv.FormatInt(realLength, 10))
	if respondWithRange {
		h.Add("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(start+claimedLength-1, 10)+"/"+strconv.FormatInt(realLength, 10))
		w.WriteHeader(206) // partial content
	} else {
		w.WriteHeader(200) // OK
	}

	io.Copy(w, reader)
}
