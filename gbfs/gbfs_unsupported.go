//go:build !linux && !freebsd
// +build !linux,!freebsd

package gbfs

func Mount(_ string, _ string, _ int64) {
	panic("gb mount is not supported on darwin")
}
