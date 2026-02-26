package proxy

import (
	"crypto/tls"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
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
	log.Println("Listening for HTTP on", listen)
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
{{if .Parent}}<a href="{{.Parent}}">Back up to {{.Parent}}</a>{{end}}
<table>
    <thead>
        <tr>
            <th>Filename</th>
            <th>Size</th>
            <th>Last modified time (filesystem)</th>
            <th>Backed up</th>
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
			<td>{{$a.SizeStr}}</td>
			<td>{{$a.FsModifiedStr}}</td>
			<td>{{$a.BackedUpStr}}</td>
		</tr>
	{{end}}
	</tbody>
</table>

</body>
</html>
`))

var pathEscaper = strings.NewReplacer("?", "%3F", "#", "%23")

func escapePath(path string) string {
	return pathEscaper.Replace(path)
}

func handleDirMaybe(w http.ResponseWriter, req *http.Request, path string, base string) {
	type Entry struct {
		Name          string
		SizeStr       string
		Odd           bool
		EscapedName   string
		FsModifiedStr string
		BackedUpStr   string
	}
	entries := make(map[Entry]struct{})
	for _, dirent := range utils.ListDirectory(path) {
		entry := Entry{
			Name: dirent.Path,
		}
		entry.Name = entry.Name[len(path):]
		if strings.Contains(entry.Name, "/") {
			entry.Name = strings.Split(entry.Name, "/")[0] + "/"
			entry.EscapedName = "/" + escapePath(path[1+len(base):]+entry.Name)
		} else {
			entry.EscapedName = "/" + escapePath(dirent.Path[1+len(base):])
			entry.SizeStr = utils.FormatCommas(dirent.Size)
			entry.FsModifiedStr = time.Unix(dirent.FsModified, 0).Format(time.RFC3339)
			entry.BackedUpStr = time.Unix(dirent.Start, 0).Format(time.RFC3339)
		}
		entries[entry] = struct{}{}
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
	parent := ""
	current := path[len(base):]
	for strings.HasSuffix(current, "/") {
		current = current[:len(current)-1]
	}
	if strings.Contains(current, "/") {
		parent = current[:strings.LastIndex(current, "/")] + "/"
	}
	err := listTemplate.Execute(w, struct {
		Match  string
		Parent string
		Rows   []Entry
	}{path, parent, keys})
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
	db.Must(err)
	ServeHashOverHTTP(hash, w, req, storage)
}
