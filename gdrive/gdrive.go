package gdrive

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strconv"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"

	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
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

func (gds *gDriveStorage) BeginBlobUpload(blobID []byte) storage_base.StorageUpload {
	pipeR, pipeW := io.Pipe()
	resultCh := make(chan gDriveResult)
	go func() {
		defer pipeR.Close()
		f := &drive.File{
			MimeType: "application/x-binary",
			Name:     hex.EncodeToString(blobID),
			Parents:  []string{gds.root},
		}
		file, err := gds.srv.Files.Create(f).Fields("id,md5Checksum,size").Media(pipeR).Do()
		if err != nil {
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
	}
}

func (gds *gDriveStorage) DownloadSection(path string, offset int64, length int64) io.ReadCloser {
	if length == 0 {
		// a range of length 0 is invalid! we get a 400 instead of an empty 200!
		return &utils.EmptyReadCloser{}
	}
	log.Println("GDrive key is", path)
	rangeStr := "bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+length-1, 10)
	log.Println("GDrive range is", rangeStr)
	getCall := gds.srv.Files.Get(path)
	getCall.Header().Set("Range", rangeStr)
	resp, err := getCall.Download()
	if err != nil {
		panic(err)
	}
	return resp.Body
}

func (up *gDriveUpload) Begin() io.Writer {
	return io.MultiWriter(up.writer, up.hasher)
}

func (up *gDriveUpload) End() storage_base.CompletedUpload {
	up.writer.Close()
	result := <-up.result
	if result.err != nil {
		panic(result.err)
	}
	file := result.file
	hash, size := up.hasher.HashAndSize()
	log.Println("Upload output", file.Id)
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

	return storage_base.CompletedUpload{
		Path:     file.Id,
		Checksum: etag,
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
	dir, err := createDir(srv, "gb", "root")

	if err != nil {
		panic(fmt.Sprintf("Could not create dir: %v\n", err))
	}
	log.Println("I have created a folder called \"gb\" in the root of this Google Drive account")
	log.Println("Since I will remember it by its ID, not by its name, you can rename it or move it wherever you want!")
	log.Println("The ID is", dir.Id)

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
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}
	return srv
}

func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	log.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

func createDir(service *drive.Service, name string, parentId string) (*drive.File, error) {
	dir := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{parentId},
	}
	file, err := service.Files.Create(dir).Do()
	if err != nil {
		log.Println("Could not create dir: " + err.Error())
		return nil, err
	}
	return file, nil
}

func parseCredentials(b []byte) *oauth2.Config {
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		log.Println("Unable to parse client secret file to config")
		panic(err)
	}
	return config
}
