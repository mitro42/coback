package catalog

import (
	"os"
	"testing"
	"time"

	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestMissingFile(t *testing.T) {
	item, err := newItem(afero.NewOsFs(), "no_such_file")
	th.NokPrefix(t, err, "Cannot open file")
	th.Assert(t, item == nil, "Item expected to be nil")
}

func TestCatalogItem(t *testing.T) {
	basePath, err := os.Getwd()
	th.Ok(t, err)
	fs := createSafeFs(basePath)
	path := "test_data/test1.txt"
	strTs := "2018-10-24T23:38:47.713775685+01:00"
	ts, err := time.Parse(time.RFC3339Nano, strTs)
	th.Ok(t, err)
	fs.Chtimes(path, ts, ts)
	item, err := newItem(fs, path)
	th.Ok(t, err)
	th.Equals(t, path, item.Path)
	th.Equals(t, int64(1160), item.Size)
	th.Equals(t, strTs, item.ModificationTime)
	th.Equals(t, Checksum("b3cd1cf6179bca32fd5d76473b129117"), item.Md5Sum)
}

func TestCatalogItem2(t *testing.T) {
	basePath, err := os.Getwd()
	th.Ok(t, err)
	fs := createSafeFs(basePath)
	path := "test_data/test2.txt"
	strTs := "2018-10-25T07:37:27.809296805+01:00"
	ts, err := time.Parse(time.RFC3339Nano, strTs)
	th.Ok(t, err)
	fs.Chtimes(path, ts, ts)
	item, err := newItem(fs, path)
	th.Ok(t, err)
	th.Equals(t, path, item.Path)
	th.Equals(t, int64(1304), item.Size)
	th.Equals(t, strTs, item.ModificationTime)
	th.Equals(t, Checksum("89b2b34c7b8d232041f0fcc1d213d7bc"), item.Md5Sum)
}
