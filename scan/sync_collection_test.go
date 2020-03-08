package scan

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestSyncCollectionCreate(t *testing.T) {
	memFs := afero.NewMemMapFs()
	collectionFs, err := InitializeFolder(memFs, "photos", "Collection")
	th.Ok(t, err)
	c, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, catalog.NewCatalog(), c)
}

func TestSyncCollectionStart(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	c, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, 4, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	checkFilesInCatalog(t, c, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}

func TestSyncCollectionWhenCatalogIsUpToDate(t *testing.T) {
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	cSynced, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)
	cRead, err := catalog.Read(collectionFs, catalog.CatalogFileName)
	th.Ok(t, err)
	cSynced2, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, cSynced, cRead)
	th.Equals(t, cSynced2, cRead)
}

func TestSyncCollectionWhenNonDuplicateFileRemovedFromDisk(t *testing.T) {
	fs := createMemFsTestData()

	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	_, err = SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	err = collectionFs.Remove("test1.txt")
	th.Ok(t, err)
	err = collectionFs.Remove("subfolder/file2.bin")
	th.Ok(t, err)

	cAfterDelete, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)
	th.Equals(t, 2, cAfterDelete.Count())
	th.Equals(t, 2, cAfterDelete.DeletedCount())
	checkFilesInCatalog(t, cAfterDelete, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cAfterDelete, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
	th.Equals(t, true, cAfterDelete.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117"))  // test1.txt
	th.Equals(t, true, cAfterDelete.IsDeletedChecksum("f350c40373648527aa95b15786473501"))  // subfolder/file2.bin
	th.Equals(t, false, cAfterDelete.IsDeletedChecksum("89b2b34c7b8d232041f0fcc1d213d7bc")) // test2.txt
	th.Equals(t, false, cAfterDelete.IsDeletedChecksum("1cb0bad847fb90f95a767854932ec7c4")) // subfolder/file1.bin
}

func TestSyncCollectionWhenDuplicateFileRemovedFromDisk(t *testing.T) {
	fs := createMemFsTestData()

	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	item, err := catalog.NewItem(collectionFs, "test1.txt")
	th.Ok(t, err)
	fsh.CopyFile(collectionFs, item.Path, item.ModificationTime, afero.NewBasePathFs(collectionFs, "subfolder"))

	cOrig, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, 5, cOrig.Count())
	th.Equals(t, 0, cOrig.DeletedCount())
	checkFilesInCatalog(t, cOrig, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cOrig, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, cOrig, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, cOrig, "subfolder/test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, cOrig, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")

	err = collectionFs.Remove("test1.txt")
	th.Ok(t, err)
	cAfterDelete, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)
	th.Equals(t, 4, cAfterDelete.Count())
	th.Equals(t, 0, cAfterDelete.DeletedCount())
	checkFilesInCatalog(t, cAfterDelete, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cAfterDelete, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, cAfterDelete, "subfolder/test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, cAfterDelete, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")

	err = collectionFs.Remove("subfolder/test1.txt")
	th.Ok(t, err)
	cAfterDelete2, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)
	th.Equals(t, 3, cAfterDelete2.Count())
	th.Equals(t, 1, cAfterDelete2.DeletedCount())
	checkFilesInCatalog(t, cAfterDelete2, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cAfterDelete2, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, cAfterDelete2, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
	th.Equals(t, true, cAfterDelete2.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117")) // test1.txt
}

func TestSyncCollectionWhenFileAddedToDisk(t *testing.T) {
	fs := createMemFsTestData()

	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	_, err = SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	dummy0 := dummies[0]
	createDummyFile(collectionFs, dummy0)

	cAfterAdd, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, 5, cAfterAdd.Count())
	th.Equals(t, 0, cAfterAdd.DeletedCount())
	checkFilesInCatalog(t, cAfterAdd, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cAfterAdd, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, cAfterAdd, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, cAfterAdd, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
	checkFilesInCatalog(t, cAfterAdd, dummy0.Path, dummy0.Size, dummy0.Md5Sum)
}

func TestSyncCollectionWhenFileWithDeletedChecksumAddedToDisk(t *testing.T) {
	fs := createMemFsTestData()

	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	cOrig, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	dummy0 := dummies[0]
	cOrig.DeleteChecksum(dummy0.Md5Sum)
	th.Equals(t, true, cOrig.IsDeletedChecksum(dummy0.Md5Sum))
	cOrig.Write(collectionFs)

	cRead, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)

	err = createDummyFile(collectionFs, dummy0)
	th.Ok(t, err)
	cModified, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, 5, cModified.Count())
	th.Equals(t, 0, cModified.DeletedCount())
	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cModified, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
	checkFilesInCatalog(t, cModified, dummy0.Path, dummy0.Size, dummy0.Md5Sum)
	th.Equals(t, false, cModified.IsDeletedChecksum(dummy0.Md5Sum))
}

func TestSyncCollectionWhenFileModifiedOnDisk(t *testing.T) {
	fs := createMemFsTestData()

	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	_, err = SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	// overwrite test1.txt with the dummy0
	dummy0 := dummies[0]
	dummy0.Path = "test1.txt"
	err = collectionFs.Remove("test1.txt")
	th.Ok(t, err)
	err = createDummyFile(collectionFs, dummy0)
	th.Ok(t, err)

	cModified, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, 4, cModified.Count())
	th.Equals(t, 0, cModified.DeletedCount())
	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cModified, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, cModified, "test1.txt", dummy0.Size, dummy0.Md5Sum)
	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}

func TestSyncCollectionWhenFileModifiedOnDiskToHaveADeletedCheckSum(t *testing.T) {
	fs := createMemFsTestData()

	collectionFs, err := InitializeFolder(fs, "test_data", "Collection")
	th.Ok(t, err)
	cOrig, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	// delete the checksum of dummy0 and save new catalog
	dummy0 := dummies[0]
	cOrig.DeleteChecksum(dummy0.Md5Sum)
	th.Equals(t, true, cOrig.IsDeletedChecksum(dummy0.Md5Sum))
	cOrig.Write(collectionFs)
	cRead, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)

	// overwrite test1.txt with the dummy0
	dummy0.Path = "test1.txt"
	err = collectionFs.Remove("test1.txt")
	th.Ok(t, err)
	err = createDummyFile(collectionFs, dummy0)
	th.Ok(t, err)

	cModified, err := SyncCatalogWithCollectionFolder(collectionFs)
	th.Ok(t, err)

	th.Equals(t, 4, cModified.Count())
	th.Equals(t, 0, cModified.DeletedCount())
	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, cModified, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, cModified, "test1.txt", dummy0.Size, dummy0.Md5Sum)
	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}
