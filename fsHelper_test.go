package main

import (
	"os"
	"strconv"
	"testing"

	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestNextUnusedFolder(t *testing.T) {
	fs := afero.NewMemMapFs()

	th.Equals(t, "1", nextUnusedFolder(fs))
	th.Equals(t, "1", nextUnusedFolder(fs))
	fs.Mkdir("1", 0755)
	th.Equals(t, "2", nextUnusedFolder(fs))
	fs.MkdirAll("3/some/other", 0755)
	th.Equals(t, "2", nextUnusedFolder(fs))
	fs.MkdirAll("2/subdir", 0755)
	th.Equals(t, "4", nextUnusedFolder(fs))
	f, err := fs.Create("4")
	th.Ok(t, err)
	f.Close()
	th.Equals(t, "5", nextUnusedFolder(fs))
	for i := 5; i <= 102; i++ {
		fs.MkdirAll(strconv.Itoa(i), 0755)
		th.Equals(t, strconv.Itoa(i+1), nextUnusedFolder(fs))
	}
}

func TestEnsureDirectoryExist(t *testing.T) {
	fs := afero.NewMemMapFs()

	_, err := fs.Stat("test1")
	th.Equals(t, true, os.IsNotExist(err))
	// directory is created if doesn't exist
	err = ensureDirectoryExist(fs, "test1")
	th.Ok(t, err)
	fi, err := fs.Stat("test1")
	th.Ok(t, err)
	th.Equals(t, true, fi.IsDir())

	// no error if directory already exists
	err = ensureDirectoryExist(fs, "test1")
	th.Ok(t, err)

	f, err := fs.Create("test2")
	th.Ok(t, err)
	f.Close()
	err = ensureDirectoryExist(fs, "test2")
	th.NokPrefix(t, err, "Path is a file")
}
