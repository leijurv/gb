package utils

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"testing"
)

type jitterReader struct {
	r        *bytes.Reader
	rand     *rand.Rand
	maxChunk int
}

func (jr *jitterReader) Read(p []byte) (int, error) {
	if jr.r.Len() == 0 {
		return 0, io.EOF
	}
	chunk := 1 + jr.rand.Intn(jr.maxChunk)
	if chunk > len(p) {
		chunk = len(p)
	}
	buf := make([]byte, chunk)
	n, err := jr.r.Read(buf)
	if n > 0 {
		copy(p, buf[:n])
	}
	return n, err
}

func newJitterReader(seed int64, data []byte) io.Reader {
	return &jitterReader{
		r:        bytes.NewReader(data),
		rand:     rand.New(rand.NewSource(seed)),
		maxChunk: 64,
	}
}

type errorAfterReader struct {
	r       io.Reader
	remain  int
	errOnce bool
	err     error
}

func (er *errorAfterReader) Read(p []byte) (int, error) {
	if er.errOnce {
		return 0, er.err
	}
	if er.remain <= 0 {
		er.errOnce = true
		return 0, er.err
	}
	if len(p) > er.remain {
		p = p[:er.remain]
	}
	n, err := er.r.Read(p)
	er.remain -= n
	if err != nil {
		return n, err
	}
	if er.remain <= 0 {
		er.errOnce = true
		return n, er.err
	}
	return n, nil
}

func TestReadersJitter(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		data := []byte("some bytes for comparison")
		left := newJitterReader(1, data)
		right := newJitterReader(2, data)
		different, err := Readers(left, right)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if different {
			t.Fatalf("expected equal readers")
		}
	})
	t.Run("same-length-different-content", func(t *testing.T) {
		left := newJitterReader(3, []byte("abcdefg123456"))
		right := newJitterReader(4, []byte("abcXefg123456"))
		different, err := Readers(left, right)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !different {
			t.Fatalf("expected different readers")
		}
	})
	t.Run("different-length", func(t *testing.T) {
		left := newJitterReader(5, []byte("short"))
		right := newJitterReader(6, []byte("shorter? nope, longer"))
		different, err := Readers(left, right)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !different {
			t.Fatalf("expected different readers")
		}
	})
	t.Run("passes-through-error", func(t *testing.T) {
		data := []byte("some bytes for error case")
		sentinel := errors.New("read error")
		left := &errorAfterReader{
			r:      newJitterReader(7, data),
			remain: 8,
			err:    sentinel,
		}
		right := newJitterReader(8, data)
		_, err := Readers(left, right)
		if !errors.Is(err, sentinel) {
			t.Fatalf("expected error %v, got %v", sentinel, err)
		}
	})
}
