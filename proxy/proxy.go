package proxy

import (
	"crypto/tls"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/leijurv/gb/storage"

	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage_base"
)

func Proxy(label string) {
	if label == "" {
		log.Println("First, we need to pick a storage to fetch em from")
		log.Println("Options:")
		descs := storage.GetAllDescriptors()
		for _, d := range descs {
			var label string
			err := db.DB.QueryRow("SELECT readable_label FROM storage WHERE storage_id = ?", d.StorageID[:]).Scan(&label)
			if err != nil {
				panic(err)
			}
			log.Println("•", d.Kind, d.RootPath, "To use this one, do `gb proxy --label=\""+label+"\"`")
		}
		return
	}
	storage.GetAll()
	var storageID []byte
	err := db.DB.QueryRow("SELECT storage_id FROM storage WHERE readable_label = ?", label).Scan(&storageID)
	if err != nil {
		panic(err)
	}
	storage := storage.GetByID(storageID)
	log.Println("Using storage:", storage)
	server := &http.Server{
		Addr: ":7893",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleHTTP(w, r, storage)
		}),
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)), // disables http/2
	}
	log.Fatal(server.ListenAndServe())
}

var listTemplate = template.Must(template.New("list").Parse(`
<html>
<head>
<title>Directory listing for /</title>
<style>
.even-dir { background-color: #efe0ef }
.even { background-color: #eee }
.odd-dir {background-color: #f0d0ef }
.odd { background-color: #dedede }
.icon { text-align: center }
.listing {
    margin-left: auto;
    margin-right: auto;
    width: 50%;
    padding: 0.1em;
    }

body { border: 0; padding: 0; margin: 0; background-color: #efefef; }
h1 {padding: 0.1em; background-color: #777; color: white; border-bottom: thin white dashed;}

</style>
</head>

<body>
<h1>Paths beginning with {{.Match}}</h1>
<table>
    <thead>
        <tr>
            <th>Filename</th>
            <th>Size</th>
        </tr>
    </thead>
    <tbody>
	{{range $i, $a := .Rows}}
		{{if $a.Odd}}
		<tr class="odd">
		{{else}}
		<tr class="even">
		{{end}}
			<td><a href="{{$a.EscapedName}}">{{$a.Name}}</a></td>
			<td>{{if eq $a.Size -1}}{{else}}{{$a.Size}}{{end}}</td>
		</tr>
	{{end}}
	</tbody>
</table>

</body>
</html>
`))

func handleDirMaybe(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	rows, err := db.DB.Query("SELECT path, size FROM files INNER JOIN sizes ON sizes.hash = files.hash WHERE end IS NULL AND path GLOB ?", path+"*")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	type Entry struct {
		Name        string
		Size        int64
		Odd         bool
		EscapedName string
	}
	entries := make(map[Entry]struct{})
	for rows.Next() {
		var match string
		var size int64
		err := rows.Scan(&match, &size)
		if err != nil {
			panic(err)
		}
		entry := Entry{
			Name: match,
			Size: size,
		}
		entry.Name = entry.Name[len(path):]
		if strings.Contains(entry.Name, "/") {
			entry.Name = strings.Split(entry.Name, "/")[0] + "/"
			entry.Size = -1
			entry.EscapedName = "/" + url.PathEscape(path[1:]+entry.Name)
		} else {
			entry.EscapedName = "/" + url.PathEscape(match[1:])
		}
		entries[entry] = struct{}{}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	keys := make([]Entry, 0)
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i int, j int) bool {
		return keys[i].Name < keys[j].Name
	})
	for i := range keys {
		keys[i].Odd = i%2 == 1
	}
	err = listTemplate.Execute(w, struct {
		Match string
		Rows  []Entry
	}{path, keys})
	if err != nil {
		panic(err)
	}
}

func handleHTTP(w http.ResponseWriter, req *http.Request, storage storage_base.Storage) {
	pathOnDisk := req.URL.Path
	log.Println("Request is for", pathOnDisk)
	var hash []byte
	err := db.DB.QueryRow("SELECT hash FROM files WHERE path = ? AND end IS NULL", pathOnDisk).Scan(&hash)
	if err == db.ErrNoRows {
		handleDirMaybe(w, req)
		return
	}
	if err != nil {
		panic(err)
	}
	var realContentLength int64
	err = db.DB.QueryRow("SELECT size FROM sizes WHERE hash = ?", hash).Scan(&realContentLength)
	if err != nil {
		panic(err)
	}
	var blobID []byte
	var path string
	var key []byte
	var offsetIntoBlob int64
	var comp string
	err = db.DB.QueryRow(
		"SELECT blob_entries.blob_id, blobs.encryption_key, blob_storage.path, blob_entries.offset, blob_entries.compression_alg FROM blob_entries INNER JOIN blob_storage ON blob_storage.blob_id = blob_entries.blob_id INNER JOIN blobs ON blobs.blob_id = blob_storage.blob_id WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?",
		hash, storage.GetID()).Scan(&blobID, &key, &path, &offsetIntoBlob, &comp)
	if err != nil {
		panic(err)
	}
	if comp != "" {
		http.Error(w, "this blob entry is compressed, random seeking is not currently supported for compression sorry", http.StatusServiceUnavailable)
		return
	}
	log.Println(req)
	log.Println("Offset into blob", offsetIntoBlob)
	claimedLength := realContentLength
	seekStart := offsetIntoBlob
	var requestedStart int64
	respondWithRange := false
	if _, ok := req.Header["Range"]; ok {
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

	var resp *http.Response
	if path[:3] == "gb/" {
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
		resp, err = http.DefaultTransport.RoundTrip(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	} else {
		resp = storage.DownloadSectionHTTP(path, seekStart, claimedLength)
	}
	defer resp.Body.Close()
	/*allowedHeaders := map[string]bool {
		"Date": true,
		"Content-Length": true,
		"Content-Range": true,
		"Content-Type": true,
		"Accept-Ranges": true,
	}*/
	for k, vv := range resp.Header {
		for _, v := range vv {
			if k == "Content-Length" {
				log.Println("Intercepted content length reply:", k, v)
				v = strconv.FormatInt(claimedLength, 10)
				log.Println("Overridden to", v)
				if !respondWithRange {
					continue
				}
			}
			if k == "Content-Range" {
				log.Println("Intercepted content range reply:", k, v)
				v = "bytes " + strconv.FormatInt(requestedStart, 10) + "-" + strconv.FormatInt(requestedStart+claimedLength-1, 10) + "/" + strconv.FormatInt(realContentLength, 10)
				log.Println("Overridden to", v)
				if !respondWithRange {
					continue
				}
			}
			if k == "Content-Type" {
				if strings.HasSuffix(strings.ToLower(pathOnDisk), ".mp4") {
					v = "video/mp4"
				}
				if strings.HasSuffix(strings.ToLower(pathOnDisk), ".mkv") {
					v = "video/x-matroska"
				}
				if strings.HasSuffix(strings.ToLower(pathOnDisk), ".png") {
					v = "image/png"
				}
				log.Println("Content type")
			}
			if k == "Content-Disposition" {
				continue
			}
			/*if !allowedHeaders[k] {
				continue
			}*/
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Accept-Ranges", "bytes")
	log.Println("Response headers", w.Header())
	status := resp.StatusCode
	log.Println("Response status code", status)
	if !respondWithRange && status == 206 {
		log.Println("Overwriting 206 to 200 because the client did not ask for a Range")
		status = 200
	}
	w.WriteHeader(status)
	io.Copy(w, crypto.DecryptBlobEntry(io.LimitReader(resp.Body, claimedLength), seekStart, key))
}
