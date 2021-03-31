package backup

import (
	"reflect"
	"testing"
)

func TestGetDirectoriesToScan(t *testing.T) {
	{
		input := "/"
		includes := []string{"/foo/bar/", "/baz"}
		result := getDirectoriesToScan(input, includes)
		if !reflect.DeepEqual(result, includes) {
			t.Error("wrong result")
		}
	}

	{
		input := "/foo/bar/uwu"
		includes := []string{"/foo/bar/", "/uwu"}
		result := getDirectoriesToScan(input, includes)
		if !reflect.DeepEqual(result, []string{input}) {
			t.Error("wrong result")
		}
	}

	{
		input := "/foo/bar"
		includes := []string{"/foo/bar/", "/uwu"}
		result := getDirectoriesToScan(input, includes)
		if !reflect.DeepEqual(result, []string{"/foo/bar/"}) {
			t.Error("wrong result")
		}
	}
}
