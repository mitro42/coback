package catalog

import (
	"os"
	"testing"

	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestEmptyFoldersCataglogIsEmpty(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("root", 0755)
	c := NewCatalog()
	ScanFolder(c, fs, "root")
	th.Equals(t, c.Count(), 0)
	th.Equals(t, c.DeletedCount(), 0)
}

func checkFilesInCatalog(t *testing.T, c Catalog, path string, size int64, md5sum string) {
	deleted, err := c.IsDeletedPath(path)
	th.Ok(t, err)
	th.Equals(t, false, deleted)
	deleted, err = c.IsDeletedChecksum(md5sum)
	th.Ok(t, err)
	th.Equals(t, false, deleted)

	item1, err := c.Item(path)
	th.Ok(t, err)
	th.Equals(t, path, item1.Path)
	th.Equals(t, md5sum, item1.Md5Sum)
	th.Equals(t, size, item1.Size)
}

func TestScanOneLevelFolder(t *testing.T) {
	rootFolder := "test_data"
	fs := afero.NewBasePathFs(afero.NewOsFs(), rootFolder)
	c := NewCatalog()
	ScanFolder(c, fs, "subfolder")

	th.Equals(t, c.Count(), 2)
	th.Equals(t, c.DeletedCount(), 0)

	checkFilesInCatalog(t, c, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
}

func TestScanRecursive(t *testing.T) {
	rootFolder, _ := os.Getwd()
	fs := afero.NewBasePathFs(afero.NewOsFs(), rootFolder)
	c := NewCatalog()
	ScanFolder(c, fs, "test_data")

	th.Equals(t, c.Count(), 4)
	th.Equals(t, c.DeletedCount(), 0)
	checkFilesInCatalog(t, c, "test_data/subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "test_data/subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c, "test_data/test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c, "test_data/test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}
