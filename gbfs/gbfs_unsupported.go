//go:build !linux && !freebsd
// +build !linux,!freebsd

package gbfs

import "github.com/leijurv/gb/storage_base"

func Mount(_ string, _ string, _ int64, _ storage_base.Storage) {
	panic("gb mount is not supported on darwin")
}
