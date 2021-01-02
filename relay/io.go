package relay

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
)

func readJSON(in io.Reader, val interface{}) {
	data := readData(in)
	log.Println("IO reading json", string(data))
	err := json.Unmarshal(data, val)
	if err != nil {
		panic(err)
	}
}

func readData(in io.Reader) []byte {
	var len int64
	err := binary.Read(in, binary.BigEndian, &len)
	if err != nil {
		panic(err)
	}
	data := make([]byte, len)
	n, err := io.ReadFull(in, data)
	if err != nil || int64(n) != len {
		panic(err)
	}
	return data
}

func writeJSON(out io.Writer, val interface{}) {
	data, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	log.Println("IO writing json", string(data))
	writeData(out, data)
}

func writeData(out io.Writer, data []byte) {
	err := binary.Write(out, binary.BigEndian, int64(len(data)))
	if err != nil {
		panic(err)
	}
	n, err := out.Write(data)
	if err != nil || n != len(data) {
		panic(err)
	}
}
