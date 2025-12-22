package utils

import (
	"bytes"
	"io"
)

// Readers compares two readers and returns true if they differ.
func Readers(left, right io.Reader) (bool, error) {
	bufLeft := make([]byte, 32*1024)
	bufRight := make([]byte, 32*1024)
	leftState := readState{}
	rightState := readState{}
	for {
		fillState(&leftState, left, bufLeft)
		fillState(&rightState, right, bufRight)

		if len(leftState.buf) == 0 && leftState.pendingErr != nil {
			return false, leftState.pendingErr
		}
		if len(rightState.buf) == 0 && rightState.pendingErr != nil {
			return false, rightState.pendingErr
		}

		if len(leftState.buf) == 0 && len(rightState.buf) == 0 {
			if leftState.eof && rightState.eof {
				return false, nil
			}
			if leftState.eof != rightState.eof {
				return true, nil
			}
			continue
		}

		if len(leftState.buf) == 0 || len(rightState.buf) == 0 {
			if leftState.eof || rightState.eof {
				return true, nil
			}
			continue
		}

		minLen := len(leftState.buf)
		if len(rightState.buf) < minLen {
			minLen = len(rightState.buf)
		}
		if !bytes.Equal(leftState.buf[:minLen], rightState.buf[:minLen]) {
			return true, nil
		}
		leftState.buf = leftState.buf[minLen:]
		rightState.buf = rightState.buf[minLen:]
	}
}

type readState struct {
	buf        []byte
	eof        bool
	pendingErr error
}

func fillState(state *readState, r io.Reader, scratch []byte) {
	if state.eof || state.pendingErr != nil || len(state.buf) != 0 {
		return
	}
	n, err := r.Read(scratch)
	if n > 0 {
		state.buf = scratch[:n]
	}
	if err == nil {
		return
	}
	if err == io.EOF {
		state.eof = true
		return
	}
	if n > 0 {
		state.pendingErr = err
		return
	}
	state.pendingErr = err
}
