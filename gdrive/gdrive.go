package gdrive

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

type gDriveStorage struct {
	storageID []byte
	srv       *drive.Service
	root      string
}

func LoadGDriveStorageInfoFromDatabase(storageID []byte, identifier string, rootPath string) storage_base.Storage {
	return &gDriveStorage{
		storageID: storageID,
		srv:       driveServiceFromIdentifier(identifier),
		root:      rootPath,
	}
}

func (gds *gDriveStorage) GetID() []byte {
	return gds.storageID
}

func (gds *gDriveStorage) BeginDatabaseUpload(filename string) storage_base.StorageUpload {
	return gds.beginUpload(nil, filename)
}

func (gds *gDriveStorage) BeginBlobUpload(blobID []byte) storage_base.StorageUpload {
	return gds.beginUpload(blobID, hex.EncodeToString(blobID))
}

func (gds *gDriveStorage) beginUpload(blobIDOptional []byte, filename string) *gDriveUpload {
	pipeR, pipeW := io.Pipe()
	resultCh := make(chan gDriveResult)
	go func() {
		defer pipeR.Close()
		file, err := gds.srv.Files.Create(&drive.File{
			MimeType: "application/x-binary",
			Name:     filename,
			Parents:  []string{gds.root},
		}).Fields("id, md5Checksum, size, name").Media(pipeR).Do()
		if err != nil {
			pipeR.CloseWithError(err)
			log.Println("gdrive error", err)
		}
		resultCh <- gDriveResult{file, err}
	}()
	hs := utils.NewMD5HasherSizer()
	return &gDriveUpload{
		writer: pipeW,
		hasher: &hs,
		result: resultCh,
		gds:    gds,
		blobID: blobIDOptional,
	}
}

func (gds *gDriveStorage) DownloadSection(path string, offset int64, length int64) io.ReadCloser {
	return gds.DownloadSectionHTTP(path, offset, length).Body
}

func (gds *gDriveStorage) DownloadSectionHTTP(path string, offset int64, length int64) *http.Response {
	if length == 0 {
		// a range of length 0 is invalid! we get a 400 instead of an empty 200!
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader(nil)),
		}
	}
	log.Println("GDrive key is", path)
	rangeStr := utils.FormatHTTPRange(offset, length)
	log.Println("GDrive range is", rangeStr)
	getCall := gds.srv.Files.Get(path)
	getCall.Header().Set("Range", rangeStr)
	resp, err := getCall.Download()
	if err != nil {
		panic(err)
	}
	return resp
}

func (gds *gDriveStorage) Metadata(path string) (string, int64) {
	file, err := gds.srv.Files.Get(path).Fields("md5Checksum, size").Do()
	if err != nil {
		panic(err)
	}
	return file.Md5Checksum, file.Size
}

func (gds *gDriveStorage) ListBlobs() []storage_base.UploadedBlob {
	log.Println("Listing blobs in", gds)
	log.Println("Requesting pages of size 1000, which is the maximum. This can take 20+ seconds per page because their API is super slow :(")
	// increasing pagesize made this *slower*
	// also 100 gives enough progress that people will realize it's working
	query := gds.srv.Files.List().PageSize(1000).Q("'" + gds.root /* inb4 gdrive query injection */ + "' in parents and trashed = false").Fields("nextPageToken, files(id, md5Checksum, size, name)")
	files := make([]storage_base.UploadedBlob, 0)
	for {
		r, err := query.Do()
		if err != nil {
			panic(err)
		}
		for _, i := range r.Files {
			if strings.HasPrefix(i.Name, "db-backup-") || strings.HasPrefix(i.Name, "db-v2backup-") {
				continue // this is not a blob
			}
			blobID, err := hex.DecodeString(i.Name)
			if err != nil || len(blobID) != 32 {
				panic("Unexpected file not following GB naming convention \"" + i.Name + "\" Google Drive file ID: " + i.Id)
			}
			files = append(files, storage_base.UploadedBlob{
				StorageID: gds.storageID,
				Path:      i.Id,
				Checksum:  i.Md5Checksum,
				Size:      i.Size,
				BlobID:    blobID,
			})
		}
		if r.NextPageToken == "" {
			break
		} else {
			query.PageToken(r.NextPageToken)
		}
		log.Println("Fetched page from Google Drive. Have", len(files), "blobs so far")
	}
	log.Println("Listed", len(files), "blobs in Google Drive")
	return files
}

func (gds *gDriveStorage) DeleteBlob(path string) {
	log.Println("Deleting Google Drive file at path:", path)
	err := gds.srv.Files.Delete(path).Do()
	if err != nil {
		panic("Error deleting Google Drive file: " + err.Error())
	}
	log.Println("Successfully deleted Google Drive file:", path)
}

func (gds *gDriveStorage) String() string {
	return "Google Drive StorageID " + hex.EncodeToString(gds.storageID[:])
}

func (up *gDriveUpload) Writer() io.Writer {
	return io.MultiWriter(up.writer, up.hasher)
}

func (up *gDriveUpload) End() storage_base.UploadedBlob {
	up.writer.Close()
	result := <-up.result
	if result.err != nil {
		panic(result.err)
	}
	file := result.file
	hash, size := up.hasher.HashAndSize()
	log.Println("Upload output: Name:", file.Name, "ID:", file.Id)
	if size != file.Size {
		log.Println("Expecting size", size, "actual size", file.Size)
		panic("gdrive broke the size lmao")
	}
	etag := hex.EncodeToString(hash)
	log.Println("Expecting etag", etag)
	real := file.Md5Checksum
	log.Println("Real etag was", real)
	if etag != real {
		panic("gdrive broke the etag lmao")
	}

	return storage_base.UploadedBlob{
		StorageID: up.gds.storageID,
		Path:      file.Id,
		Checksum:  etag,
		Size:      file.Size,
		BlobID:    up.blobID,
	}
}

type gDriveResult struct {
	file *drive.File
	err  error
}

type gDriveUpload struct {
	writer *io.PipeWriter
	result chan gDriveResult
	hasher *utils.HasherSizer
	gds    *gDriveStorage
	blobID []byte
}

type identifierInDB struct {
	Token       *oauth2.Token `json:"token"`
	Credentials string        `json:"credentials"`
}

func CreateNewGDriveStorage() (identifier, rootPath string) {
	b, err := ioutil.ReadFile("credentials.json")
	if err != nil {
		panic("You need to get your Drive API credentials file and put them in credentials.json, sorry. Enable the Drive API at https://developers.google.com/drive/api/v3/quickstart/go")
	}
	config := parseCredentials(b)
	tok := getTokenFromWeb(config)
	id, err := json.Marshal(identifierInDB{
		Credentials: string(b),
		Token:       tok,
	})
	if err != nil {
		panic(err) // literally 0 reason why json marshaling could fail
	}
	log.Println("Authentication complete. Identifier blob is ", string(id))
	srv := driveServiceFromIdentifier(string(id))
	dir := createDir(srv, "gb", "root")

	log.Println("I have created a folder called \"gb\" in the root of this Google Drive account")
	log.Println("Since I will remember it by its ID, not by its name, you can rename it or move it wherever you want, without breaking anything!")
	log.Println("This means that, UNLIKE in rclone, you CAN'T \"transplant\" the files into a new folder and call that one gb. (well, you can, but you'd have to modify the storage table in the database lol)")
	log.Println("The ID is", dir.Id)
	log.Println("Furthermore, the name of each file also doesn't matter at all. You can furthermore furthermore move the files anywhere (even out of the gb folder), BUT that will cause the \"paranoia storage\" command to fail, since it just lists files in your gb folder. If you don't plan to use that command, everything else (like retrieval) will work fine.")
	return string(id), dir.Id
}

func driveServiceFromIdentifier(identifier string) *drive.Service {
	ident := &identifierInDB{}
	err := json.Unmarshal([]byte(identifier), ident)
	if err != nil {
		panic(err)
	}
	config := parseCredentials([]byte(ident.Credentials))
	client := config.Client(context.Background(), ident.Token)
	srv, err := drive.New(client)
	if err != nil {
		log.Println("Unable to retrieve Drive client")
		panic(err)
	}
	return srv
}

func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	log.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Println("Unable to read authorization code")
		panic(err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Println("Unable to retrieve token from web")
		panic(err)
	}
	return tok
}

func createDir(service *drive.Service, name string, parentId string) *drive.File {
	dir := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{parentId},
	}
	file, err := service.Files.Create(dir).Do()
	if err != nil {
		log.Println("Could not create dir")
		panic(err)
	}
	return file
}

func parseCredentials(b []byte) *oauth2.Config {
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		log.Println("Unable to parse client secret file to config")
		panic(err)
	}
	return config
}
