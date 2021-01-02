package relay

import (
	"bytes"
	"io"
	"log"
	"net"
	"strconv"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

// Uploading to two different places at once will halve your speed, under normal conditions.
// (assuming your constraint is your upload speed)

// Using gb relay lets you upload at normal speed to a "relay server" (perhaps a VPS you own)
// which will then "split" that data and upload it to all N places

func RemoteSplitter() (backup.UploadServiceFactory, bool) {
	port := config.Config().RelayServerPort
	if port == -1 {
		return nil, false
	}
	ch := make(backup.UploadServiceFactory)
	desc := storage.GetAllDescriptors()
	go func() {
		for {
			ch <- connectToRelaySplitter(port, desc)
		}
	}()
	return ch, true
}

func connectToRelaySplitter(port int, desc []storage.StorageDescriptor) *remoteSplitterRelayedUploadService {
	conn, err := net.Dial("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
	marshalDescriptors(conn, desc)
	return &remoteSplitterRelayedUploadService{
		storages: storage.ResolveDescriptors(desc),
		conn:     conn,
	}
}

type remoteSplitterRelayedUploadService struct {
	conn        net.Conn
	storages    []storage_base.Storage
	blobIDCache []byte
}

type remoteSplitterRelayedUploadServiceProxiedWriter struct {
	service *remoteSplitterRelayedUploadService
}

func (rsruspw *remoteSplitterRelayedUploadServiceProxiedWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	writeData(rsruspw.service.conn, data)
	return len(data), nil
}

func (rsrus *remoteSplitterRelayedUploadService) Begin(blobID []byte) io.Writer {
	if rsrus.blobIDCache != nil {
		panic("already in use")
	}
	writeData(rsrus.conn, blobID)
	rsrus.blobIDCache = blobID
	return &remoteSplitterRelayedUploadServiceProxiedWriter{rsrus}
}

func (rsrus *remoteSplitterRelayedUploadService) End(sha256 []byte, size int64) []storage_base.UploadedBlob {
	writeData(rsrus.conn, nil)
	writtenHash := readData(rsrus.conn)
	log.Println("Hash provided by relay: ", writtenHash)
	log.Println("Locally calculated hash:", sha256)
	if !bytes.Equal(writtenHash, sha256) {
		panic("sanity check")
	}
	var completeds []storage_base.UploadedBlob
	readJSON(rsrus.conn, &completeds)
	for i := range completeds {
		completeds[i].BlobID = rsrus.blobIDCache
		completeds[i].StorageID = rsrus.storages[i].GetID()
	}
	rsrus.blobIDCache = nil
	return completeds
}

func Listen(port int) {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	log.Println("Incoming relay", conn)
	var in io.Reader
	var out io.Writer
	in = conn
	out = conn
	descs := unmarshalDescriptors(in)
	storages := storage.ResolveDescriptors(descs)
	for {
		uploader := backup.BeginDirectUpload(storages)
		blobID := readData(in)
		if len(blobID) == 0 {
			break
		}

		upload := uploader.Begin(blobID)

		hashVerify := utils.NewSHA256HasherSizer()
		upload = io.MultiWriter(upload, &hashVerify)

		for {
			data := readData(in)
			if len(data) == 0 {
				break
			}
			upload.Write(data)
		}

		hash, size := hashVerify.HashAndSize()
		completeds := uploader.End(hash, size)

		for i := range completeds {
			a := completeds[i].StorageID
			b := storages[i].GetID()
			c := descs[i].StorageID[:]
			if !bytes.Equal(a, b) || !bytes.Equal(b, c) || !bytes.Equal(a, c) {
				log.Println(a)
				log.Println(b)
				log.Println(c)
				panic("sanity check")
			}

			if !bytes.Equal(completeds[i].BlobID, blobID) {
				log.Println(completeds[i].BlobID)
				log.Println(blobID)
				panic("sanity check")
			}
		}
		writeData(out, hash)
		writeJSON(out, completeds)
	}
}
