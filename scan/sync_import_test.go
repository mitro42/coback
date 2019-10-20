package scan

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestSyncImportCreate(t *testing.T) {
	memFs := afero.NewMemMapFs()
	importFs, err := InitializeFolder(memFs, "holiday_pictures", "Import")
	th.Ok(t, err)
	c, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	th.Equals(t, catalog.NewCatalog(), c)
}

func TestSyncImportStart(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	importFs, err := InitializeFolder(fs, "test_data", "Import")
	th.Ok(t, err)
	c, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	th.Equals(t, 4, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	checkFilesInCatalog(t, c, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}

func TestSyncImportWhenCatalogIsUpToDate(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	importFs, err := InitializeFolder(fs, "test_data", "Import")
	th.Ok(t, err)
	cSynced, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)
	cRead, err := catalog.Read(importFs, catalog.CatalogFileName)
	th.Ok(t, err)
	cSynced2, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	th.Equals(t, cSynced, cRead)
	th.Equals(t, cSynced2, cRead)
}

func TestSyncImporWhenFileRemovedFromDisk(t *testing.T) {
	fs := createMemFsTestData()

	importFs, err := InitializeFolder(fs, "test_data", "Import")
	th.Ok(t, err)
	_, err = SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	err = importFs.Remove("test1.txt")
	th.Ok(t, err)
	err = importFs.Remove("subfolder/file2.bin")
	th.Ok(t, err)

	cAfterDelete, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)
	th.Equals(t, 2, cAfterDelete.Count())
	th.Equals(t, 0, cAfterDelete.DeletedCount())
	checkFilesInCatalog(t, cAfterDelete, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cAfterDelete, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}

func TestSyncImporWhenFileAddedToDisk(t *testing.T) {
	fs := createMemFsTestData()

	importFs, err := InitializeFolder(fs, "test_data", "Import")
	th.Ok(t, err)
	_, err = SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	dummy0 := dummies[0]
	createDummyFile(importFs, dummy0)

	cAfterAdd, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	th.Equals(t, 5, cAfterAdd.Count())
	th.Equals(t, 0, cAfterAdd.DeletedCount())
	checkFilesInCatalog(t, cAfterAdd, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cAfterAdd, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, cAfterAdd, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, cAfterAdd, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
	checkFilesInCatalog(t, cAfterAdd, dummy0.Path, dummy0.Size, dummy0.Md5Sum)
}

func TestSyncImporWhenFileAddedAndDeleted(t *testing.T) {
	fs := createMemFsTestData()

	importFs, err := InitializeFolder(fs, "test_data", "Import")
	th.Ok(t, err)
	cOrig, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	dummy0 := dummies[0]
	err = createDummyFile(importFs, dummy0)
	th.Ok(t, err)

	err = importFs.Remove("subfolder/file2.bin")
	th.Ok(t, err)

	cModified, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	th.Assert(t, !reflect.DeepEqual(cOrig, cModified), "The catalogs must be different")
	th.Equals(t, 4, cModified.Count())
	th.Equals(t, 0, cModified.DeletedCount())
	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cModified, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
	checkFilesInCatalog(t, cModified, dummy0.Path, dummy0.Size, dummy0.Md5Sum)
}

func TestSyncImporWhenFileHashModified(t *testing.T) {
	fs := createMemFsTestData()

	importFs, err := InitializeFolder(fs, "test_data", "Import")
	th.Ok(t, err)
	cOrig, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	dummy0 := dummies[0]
	dummy0.Path = "test1.txt"

	err = importFs.Remove("test1.txt")
	th.Ok(t, err)
	err = createDummyFile(importFs, dummy0)
	th.Ok(t, err)

	cModified, err := SyncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	th.Assert(t, !reflect.DeepEqual(cOrig, cModified), "The catalogs must be different")
	th.Equals(t, 4, cModified.Count())
	th.Equals(t, 0, cModified.DeletedCount())
	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cModified, "test1.txt", dummy0.Size, dummy0.Md5Sum)
	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}
