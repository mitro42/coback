package catalog

import (
	"os"
	"path/filepath"
	"testing"

	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestEmptyFoldersCatalogIsEmpty(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("root", 0755)
	c := ScanFolder(fs, "root", noFilter{})
	th.Equals(t, c.Count(), 0)
	th.Equals(t, c.DeletedCount(), 0)
}

func checkFilesInCatalog(t *testing.T, c Catalog, path string, size int64, md5sum Checksum) {
	t.Helper()
	deleted := c.IsDeletedChecksum(md5sum)
	th.Equals(t, false, deleted)

	item1, err := c.Item(path)
	th.Ok(t, err)
	th.Equals(t, path, item1.Path)
	th.Equals(t, md5sum, item1.Md5Sum)
	th.Equals(t, size, item1.Size)
}

func TestScanOneLevelFolder(t *testing.T) {
	fs := createSafeFs("test_data")
	c := ScanFolder(fs, "subfolder", noFilter{})

	th.Equals(t, c.Count(), 2)
	th.Equals(t, c.DeletedCount(), 0)

	checkFilesInCatalog(t, c, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
}

func TestScanFolderRecursive(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := createSafeFs(basePath)
	c := ScanFolder(fs, "test_data", noFilter{})

	th.Equals(t, c.Count(), 4)
	th.Equals(t, c.DeletedCount(), 0)
	checkFilesInCatalog(t, c, "test_data/subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "test_data/subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c, "test_data/test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c, "test_data/test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}

func TestScanRecursive(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := createSafeFs(filepath.Join(basePath, "test_data"))
	c := Scan(fs)

	th.Equals(t, c.Count(), 4)
	th.Equals(t, c.DeletedCount(), 0)
	checkFilesInCatalog(t, c, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}

func TestScanWithExtensionFilter(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := createSafeFs(basePath)
	c := ScanFolder(fs, "test_data", ExtensionFilter("txt"))

	th.Equals(t, c.Count(), 2)
	th.Equals(t, c.DeletedCount(), 0)
	checkFilesInCatalog(t, c, "test_data/subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "test_data/subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
}

func TestScanWithExtensionFilter2(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := createSafeFs(basePath)
	c := ScanFolder(fs, "test_data", ExtensionFilter("txt", "bin"))

	th.Equals(t, c.Count(), 0)
	th.Equals(t, c.DeletedCount(), 0)
}

func TestCheckOk(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	c := Scan(fs)
	cr := Check(fs, c)
	th.Equals(t, 0, len(cr.Add))
	th.Equals(t, 0, len(cr.Delete))
	th.Equals(t, 0, len(cr.Update))
	th.Equals(t, 4, len(cr.Ok))
}

func TestCheckFileMissingFromCatalog(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("bin")
	c := ScanFolder(fs, "", filter)
	cr := Check(fs, c)
	expAdd := map[string]bool{"subfolder/file1.bin": true, "subfolder/file2.bin": true}
	th.Equals(t, expAdd, cr.Add)
	th.Equals(t, 0, len(cr.Delete))
	th.Equals(t, 0, len(cr.Update))
	th.Equals(t, 2, len(cr.Ok))
}

// This cannot be detected yet
//
// func TestCheckFileMissingFromDisk(t *testing.T) {
// 	basePath, _ := os.Getwd()
// 	path := "test_data"
// 	fs := createSafeFs(filepath.Join(basePath, path))
// 	filter := ExtensionFilter("bin")
// 	c := ScanFolder(fs, "", filter)
// 	item, _ := newCatalogItem(fs, "subfolder/file1.bin")
// 	c.Add(*item)
// 	th.Equals(t, false, Check(fs, c, filter))
// }

func TestCheckItemChecksumMismatch(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("bin")
	c := ScanFolder(fs, "", filter)
	item, err := c.Item("test1.txt")
	th.Ok(t, err)
	item.Md5Sum = "abcdef"
	err = c.Set(item)
	th.Ok(t, err)
	cr := Check(fs, c)
	expAdd := map[string]bool{"subfolder/file1.bin": true, "subfolder/file2.bin": true}
	expUpdate := map[string]bool{"test1.txt": true}
	th.Equals(t, expAdd, cr.Add)
	th.Equals(t, 0, len(cr.Delete))
	th.Equals(t, expUpdate, cr.Update)
	th.Equals(t, 1, len(cr.Ok))
}

func TestCheckItemSizeMismatch(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("bin")
	c := ScanFolder(fs, "", filter)
	item, err := c.Item("test1.txt")
	th.Ok(t, err)
	item.Size = 6854
	err = c.Set(item)
	th.Ok(t, err)
	cr := Check(fs, c)
	expAdd := map[string]bool{"subfolder/file1.bin": true, "subfolder/file2.bin": true}
	expUpdate := map[string]bool{"test1.txt": true}
	th.Equals(t, expAdd, cr.Add)
	th.Equals(t, 0, len(cr.Delete))
	th.Equals(t, expUpdate, cr.Update)
	th.Equals(t, 1, len(cr.Ok))
}

func TestCheckItemModificationTimeMismatch(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("bin")
	c := ScanFolder(fs, "", filter)
	item, err := c.Item("test1.txt")
	th.Ok(t, err)
	item.ModificationTime = "1924"
	err = c.Set(item)
	th.Ok(t, err)
	cr := Check(fs, c)
	expAdd := map[string]bool{"subfolder/file1.bin": true, "subfolder/file2.bin": true}
	expUpdate := map[string]bool{"test1.txt": true}
	th.Equals(t, expAdd, cr.Add)
	th.Equals(t, 0, len(cr.Delete))
	th.Equals(t, expUpdate, cr.Update)
	th.Equals(t, 1, len(cr.Ok))
}

func TestScanAdd(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	c := Scan(fs)

	dummy0 := dummies[0]
	dummy1 := dummies[1]
	createDummyFile(fs, dummy0)
	createDummyFile(fs, dummy1)

	checkResult := Check(fs, c)
	c2 := ScanAdd(fs, c, checkResult)

	th.Equals(t, 4, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	th.Equals(t, 6, c2.Count())
	th.Equals(t, 0, c.DeletedCount())
	checkFilesInCatalog(t, c2, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c2, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c2, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c2, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
	checkFilesInCatalog(t, c2, dummy0.Path, dummy0.Size, dummy0.Md5Sum)
	checkFilesInCatalog(t, c2, dummy1.Path, dummy1.Size, dummy1.Md5Sum)
}

// Deleted flag handling will probably change in the future
//
// func TestCheckItemDeletedFlagMismatch(t *testing.T) {
// 	basePath, _ := os.Getwd()
// 	path := "test_data"
// 	fs := createSafeFs(filepath.Join(basePath, path))
// 	filter := ExtensionFilter("bin")
// 	c := ScanFolder(fs, "", filter)
// 	item, _ := c.Item("subfolder/test1.txt")
// 	item.Deleted = true
// 	c.Add(item)
// 	th.Equals(t, false, Check(fs, c, filter))
// }
