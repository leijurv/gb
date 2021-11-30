package proxy

import (
	"crypto/tls"
	"github.com/leijurv/gb/compression"
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

func Proxy(label string, base string, listen string) {
	storage, ok := storage.StorageSelect(label)
	if !ok {
		return
	}
	if !strings.HasPrefix(base, "/") && base != "" {
		panic("invalid base")
	}
	for strings.HasSuffix(base, "/") {
		base = base[:len(base)-1]
	}
	server := &http.Server{
		Addr: listen,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleHTTP(w, r, storage, base)
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

func handleDirMaybe(w http.ResponseWriter, req *http.Request, path string, base string) {
	globPath := strings.Replace(strings.Replace(path, "[", "?", -1), "]", "?", -1) + "*"
	rows, err := db.DB.Query("SELECT path, size FROM files INNER JOIN sizes ON sizes.hash = files.hash WHERE end IS NULL AND path GLOB ?", globPath)
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
			entry.EscapedName = "/" + url.PathEscape(path[1+len(base):]+entry.Name)
		} else {
			entry.EscapedName = "/" + url.PathEscape(match[1+len(base):])
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

var playerTemplate = template.Must(template.New("player").Parse(`
<html>
<head>
<style type="text/css">
.videoBgWrapper {
    display: block;
    margin: 0 auto;
    width: 100%;
}
.videoBg{
    width: 100%;
}
</style>
<body>
<div class="videoBgWrapper">
<video controls preload=auto class="videoBg" id="myVideo" src="{{ .Path }}"></video>
</div>
</body>
</html>
`))

func handleHTTP(w http.ResponseWriter, req *http.Request, storage storage_base.Storage, base string) {
	pathOnDisk := req.URL.Path
	if !strings.HasPrefix(pathOnDisk, "/") {
		pathOnDisk = "/" + pathOnDisk
	}
	if strings.HasPrefix(pathOnDisk, "/player/") {
		pathOnDisk = pathOnDisk[len("/player"):]
		t := req.URL.Query().Get("t")
		log.Println("T is", t)
		if t != "" {
			pathOnDisk += "#t=" + t
		}
		playerTemplate.Execute(w, struct {
			Path string
		}{pathOnDisk})
		return
	}
	pathOnDisk = base + pathOnDisk
	log.Println("Request is for", pathOnDisk)
	var hash []byte
	err := db.DB.QueryRow("SELECT hash FROM files WHERE path = ? AND end IS NULL", pathOnDisk).Scan(&hash)
	if err == db.ErrNoRows {
		handleDirMaybe(w, req, pathOnDisk, base)
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
	var compressedSize int64
	var offsetIntoBlob int64
	var comp string
	err = db.DB.QueryRow(
		"SELECT blob_entries.blob_id, blobs.encryption_key, blob_storage.path, blob_entries.final_size, blob_entries.offset, blob_entries.compression_alg FROM blob_entries INNER JOIN blob_storage ON blob_storage.blob_id = blob_entries.blob_id INNER JOIN blobs ON blobs.blob_id = blob_storage.blob_id WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?",
		hash, storage.GetID()).Scan(&blobID, &key, &path, &compressedSize, &offsetIntoBlob, &comp)
	if err != nil {
		panic(err)
	}
	log.Println(req)
	log.Println("Offset into blob", offsetIntoBlob)
	claimedLength := compressedSize
	seekStart := offsetIntoBlob
	var requestedStart int64
	respondWithRange := false
	if _, ok := req.Header["Range"]; ok {
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

	var data io.ReadCloser
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
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		data = resp.Body
	} else {
		data = storage.DownloadSection(path, seekStart, claimedLength)
	}
	defer data.Close()

	decrypted := crypto.DecryptBlobEntry(io.LimitReader(data, claimedLength), seekStart, key)
	reader := compression.ByAlgName(comp).Decompress(decrypted)
	writeHttpResponse(w, reader, requestedStart, claimedLength, realContentLength, pathOnDisk, respondWithRange)
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
