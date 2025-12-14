package compression

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/leijurv/gb/utils"
)

type mockBase struct {
	name     string
	fallible bool
}

func (m *mockBase) AlgName() string {
	return m.name
}

func (m *mockBase) Fallible() bool {
	return m.fallible
}

func (m *mockBase) DecompressionTrollBashCommandIncludingThePipe() string {
	return ""
}

type mockFallibleFails struct {
	mockBase
}

func (m *mockFallibleFails) Compress(out io.Writer, in io.Reader) error {
	return errors.New("compression failed")
}

func (m *mockFallibleFails) Decompress(in io.Reader) io.ReadCloser {
	panic("should not be called")
}

var EXTRA_BYTES = []byte("extra bytes that make it bigger")

type mockFallibleMakesBigger struct {
	mockBase
}

func (m *mockFallibleMakesBigger) Compress(out io.Writer, in io.Reader) error {
	data, _ := io.ReadAll(in)
	out.Write(data)
	out.Write(EXTRA_BYTES)
	return nil
}

func (m *mockFallibleMakesBigger) Decompress(in io.Reader) io.ReadCloser {
	data, _ := io.ReadAll(in)
	trimmed := data[:len(data)-len(EXTRA_BYTES)]
	return io.NopCloser(bytes.NewReader(trimmed))
}

type mockPassthrough struct {
	mockBase
}

func (m *mockPassthrough) Compress(out io.Writer, in io.Reader) error {
	io.Copy(out, in)
	return nil
}

func (m *mockPassthrough) Decompress(in io.Reader) io.ReadCloser {
	return io.NopCloser(in)
}

type mockCorrupts struct {
	mockBase
}

func (m *mockCorrupts) Compress(out io.Writer, in io.Reader) error {
	io.Copy(out, in)
	return nil
}

func (m *mockCorrupts) Decompress(in io.Reader) io.ReadCloser {
	data, _ := io.ReadAll(in)
	if len(data) > 0 {
		data[0] ^= 0xFF
	}
	return io.NopCloser(bytes.NewReader(data))
}

func makeHasherSizerFor(data []byte) *utils.HasherSizer {
	hs := utils.NewSHA256HasherSizer()
	hs.Write(data)
	return &hs
}

func TestCompressFallibleFailsFallsBackToNext(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{
		&mockFallibleFails{mockBase{"mock_fallible_fails", true}},
		&NoCompression{},
	}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "" {
		t.Errorf("expected NoCompression (empty string), got %s", algName)
	}
	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("output doesn't match input")
	}
}

func TestCompressFallibleMakesBiggerFallsBackToNext(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{
		&mockFallibleMakesBigger{mockBase{"mock_fallible_makes_bigger", true}},
		&NoCompression{},
	}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "" {
		t.Errorf("expected NoCompression (empty string), got %s", algName)
	}
	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("output doesn't match input")
	}
}

func TestCompressFallibleWorksSucceeds(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{
		&mockPassthrough{mockBase{"mock_fallible_works", true}},
		&NoCompression{},
	}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "mock_fallible_works" {
		t.Errorf("expected mock_fallible_works, got %s", algName)
	}
	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("output doesn't match input")
	}
}

func TestCompressInfallibleWorksSucceeds(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{&mockPassthrough{mockBase{"mock_infallible_works", false}}}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "mock_infallible_works" {
		t.Errorf("expected mock_infallible_works, got %s", algName)
	}
	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("output doesn't match input")
	}
}

func TestCompressFallibleCorruptsPanics(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic but didn't get one")
		}
		if r != "compression CLAIMED it succeeded but decompressed to DIFFERENT DATA this is VERY BAD" {
			t.Errorf("unexpected panic message: %v", r)
		}
	}()

	options := []Compression{&mockCorrupts{mockBase{"mock_fallible_corrupts", true}}}
	Compress(options, &out, bytes.NewReader(input), hs)
}

func TestCompressInfallibleCorruptsPanics(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic but didn't get one")
		}
		if r != "compression CLAIMED it succeeded but decompressed to DIFFERENT DATA this is VERY BAD" {
			t.Errorf("unexpected panic message: %v", r)
		}
	}()

	options := []Compression{&mockCorrupts{mockBase{"mock_infallible_corrupts", false}}}
	Compress(options, &out, bytes.NewReader(input), hs)
}

func TestCompressFallsBackThroughMultipleFailures(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{
		&mockFallibleFails{mockBase{"mock_fallible_fails", true}},
		&mockFallibleMakesBigger{mockBase{"mock_fallible_makes_bigger", true}},
		&mockPassthrough{mockBase{"mock_fallible_works", true}},
	}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "mock_fallible_works" {
		t.Errorf("expected mock_fallible_works, got %s", algName)
	}
}

func TestCompressFallibleThenInfallible(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{
		&mockFallibleFails{mockBase{"mock_fallible_fails", true}},
		&mockPassthrough{mockBase{"mock_infallible_works", false}},
	}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "mock_infallible_works" {
		t.Errorf("expected mock_infallible_works, got %s", algName)
	}
	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("output doesn't match input")
	}
}

func TestVerifiedCompressionSucceeds(t *testing.T) {
	input := []byte("hello world this is some test data for verified compression")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	VerifiedCompression(&mockPassthrough{mockBase{"mock", false}}, &out, bytes.NewReader(input), hs)

	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("output doesn't match input")
	}
}

func TestVerifiedCompressionCorruptsPanics(t *testing.T) {
	input := []byte("hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected panic but didn't get one")
		}
		if r != "compression CLAIMED it succeeded but decompressed to DIFFERENT DATA this is VERY BAD" {
			t.Errorf("unexpected panic message: %v", r)
		}
	}()

	VerifiedCompression(&mockCorrupts{mockBase{"mock", false}}, &out, bytes.NewReader(input), hs)
}

func TestVerifiedCompressionWithLargeData(t *testing.T) {
	input := make([]byte, 1024*1024)
	for i := range input {
		input[i] = byte(i % 256)
	}
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	VerifiedCompression(&mockPassthrough{mockBase{"mock", false}}, &out, bytes.NewReader(input), hs)

	if !bytes.Equal(out.Bytes(), input) {
		t.Errorf("output doesn't match input for large data")
	}
}

func TestCompressWithEmptyInput(t *testing.T) {
	input := []byte{}
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{
		&mockPassthrough{mockBase{"mock_fallible_works", true}},
		&NoCompression{},
	}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "mock_fallible_works" {
		t.Errorf("expected mock_fallible_works, got %s", algName)
	}
	if len(out.Bytes()) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(out.Bytes()))
	}
}

func TestVerifiedCompressionWithEmptyInput(t *testing.T) {
	input := []byte{}
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	VerifiedCompression(&mockPassthrough{mockBase{"mock", false}}, &out, bytes.NewReader(input), hs)

	if len(out.Bytes()) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(out.Bytes()))
	}
}

func TestCompressWithRealZstd(t *testing.T) {
	input := []byte("hello world hello world hello world hello world")
	hs := makeHasherSizerFor(input)
	var out bytes.Buffer

	options := []Compression{&ZstdCompression{}, &NoCompression{}}
	algName := Compress(options, &out, bytes.NewReader(input), hs)

	if algName != "zstd" {
		t.Errorf("expected zstd, got %s", algName)
	}

	decompressed := (&ZstdCompression{}).Decompress(bytes.NewReader(out.Bytes()))
	defer decompressed.Close()
	result, _ := io.ReadAll(decompressed)
	if !bytes.Equal(result, input) {
		t.Errorf("zstd round-trip failed")
	}
}
