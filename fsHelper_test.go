package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/mitro42/coback/catalog"
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

	// fail if path is a file
	f, err := fs.Create("test2")
	th.Ok(t, err)
	f.Close()
	err = ensureDirectoryExist(fs, "test2")
	th.NokPrefix(t, err, "Path is a file")

	fs = afero.NewReadOnlyFs(fs)
	// fail cannot create directory
	err = ensureDirectoryExist(fs, "test3")
	th.NokPrefix(t, err, "Cannot create directory 'test3")

}

func TestCopyFile(t *testing.T) {
	sourceFs := afero.NewMemMapFs()
	destinationFs := afero.NewMemMapFs()

	testFile := func(name string, content []byte) {
		f, err := sourceFs.Create(name)
		th.Ok(t, err)
		f.Write(content)
		f.Close()

		sourceItem, err := catalog.NewItem(sourceFs, name)
		th.Ok(t, err)
		err = copyFile(sourceFs, *sourceItem, destinationFs)
		th.Ok(t, err)
		destinationItem, err := catalog.NewItem(destinationFs, name)
		th.Ok(t, err)
		th.Equals(t, sourceItem, destinationItem)
	}

	testFile("test1", []byte("some content"))
	testFile("test2", []byte("some more content"))
	th.Ok(t, sourceFs.MkdirAll("folder/structure/test", 0755))
	buf := make([]byte, 1024*1024)
	rand.Read(buf)
	testFile("folder/structure/test/big_file", buf)
}

func TestCopyFileErrors(t *testing.T) {
	sourceFs := afero.NewMemMapFs()
	destinationFs := afero.NewMemMapFs()

	// Source file doesn't exist
	f, err := sourceFs.Create("test")
	th.Ok(t, err)
	f.Close()

	sourceItem := catalog.Item{Path: "test/file"}
	th.Ok(t, err)
	err = copyFile(sourceFs, sourceItem, destinationFs)
	th.NokPrefix(t, err, "Failed to copy file 'test/file'")

	// Destination folder cannot be created
	sourceFs = afero.NewMemMapFs()
	th.Ok(t, sourceFs.Mkdir("test", 0755))

	f, err = sourceFs.Create("test/file")
	th.Ok(t, err)
	f.Write([]byte("some stuff"))
	f.Close()
	f, err = destinationFs.Create("test")
	th.Ok(t, err)
	f.Close()
	sourceItem2, err := catalog.NewItem(sourceFs, "test/file")

	err = copyFile(sourceFs, *sourceItem2, destinationFs)
	th.NokPrefix(t, err, "Failed to copy file 'test/file': Path is a file")

	// Destination fs is read only
	err = copyFile(sourceFs, *sourceItem2, afero.NewReadOnlyFs(afero.NewMemMapFs()))
	fmt.Println(err)
	th.NokPrefix(t, err, "Failed to copy file 'test/file': Cannot create directory")

	// Destination folder is read only
	destinationFs = afero.NewMemMapFs()
	th.Ok(t, destinationFs.Mkdir("test", 0755))
	err = copyFile(sourceFs, *sourceItem2, afero.NewReadOnlyFs(destinationFs))
	th.NokPrefix(t, err, "Failed to copy file 'test/file': Cannot create destination file")

	destinationFs = afero.NewMemMapFs()
	sourceItem2.ModificationTime = "Not a valid timestamp"
	err = copyFile(sourceFs, *sourceItem2, destinationFs)
	th.NokPrefix(t, err, "Failed to copy file 'test/file': Cannot parse modification time of file 'test/file")
}
