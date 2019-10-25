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

func TestSyncStagingCreate(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	memFs := afero.NewMemMapFs()
	stagingFs, err := InitializeFolder(memFs, "temp_photos", "Staging")
	th.Ok(t, err)
	c, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.Ok(t, err)

	th.Equals(t, catalog.NewCatalog(), c)
}

func TestSyncStagingStartOnlyNewFiles(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	th.Ok(t, err)
	c, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.Ok(t, err)

	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Equals(t, c, cRead)

	th.Equals(t, 4, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	checkFilesInCatalog(t, c, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}

func TestSyncStagingStartFileIsAlreadyInCollection(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	item, err := catalog.NewItem(stagingFs, "test2.txt")
	th.Ok(t, err)
	collectionCatalog.Add(*item)

	c, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.NokPrefix(t, err, "File is already in the collection")
	th.Equals(t, nil, c)

	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.NokPrefix(t, err, "Cannot read catalog")
	th.Equals(t, nil, cRead)
}

func TestSyncStagingStartFileIsAlreadyDeletedInCollection(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	collectionCatalog.DeleteChecksum("f350c40373648527aa95b15786473501") // subfolder/file2.bin

	c, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.NokPrefix(t, err, "File is already deleted from the collection")
	th.Equals(t, nil, c)

	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.NokPrefix(t, err, "Cannot read catalog")
	th.Equals(t, nil, cRead)
}

func TestSyncStagingWhenCatalogIsUpToDate(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	basePath, _ := os.Getwd()
	fs := fsh.CreateSafeFs(filepath.Dir(basePath))
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	th.Ok(t, err)
	cSynced, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	cSynced.DeleteChecksum("a")
	cSynced.DeleteChecksum("42")
	cSynced.Write(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	cSynced2, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.Ok(t, err)

	th.Equals(t, cSynced, cRead)
	th.Equals(t, cSynced2, cRead)
	th.Equals(t, true, cSynced2.IsDeletedChecksum("a"))
	th.Equals(t, true, cSynced2.IsDeletedChecksum("42"))
	th.Equals(t, false, cSynced2.IsDeletedChecksum("43"))
}

func TestSyncStagingWhenFileIsMovedToCollection(t *testing.T) {
	collectionFs, err := InitializeFolder(afero.NewMemMapFs(), ".", "Collection")
	th.Ok(t, err)

	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	th.Ok(t, err)
	Scan(stagingFs)
	th.Ok(t, err)
	movedItem, err := catalog.NewItem(stagingFs, "subfolder/file1.bin")
	th.Ok(t, err)
	err = fsh.CopyFile(stagingFs, movedItem.Path, movedItem.ModificationTime, collectionFs)
	th.Ok(t, err)
	err = stagingFs.Remove(movedItem.Path)
	th.Ok(t, err)
	collectionCatalog, err := SyncCatalogWithCollectionFolder(collectionFs)
	collectionClone := collectionCatalog.Clone()
	th.Ok(t, err)

	cSynced, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.Ok(t, err)

	th.Equals(t, false, cSynced.IsDeletedChecksum(movedItem.Md5Sum))
	th.Equals(t, false, cSynced.IsKnownChecksum(movedItem.Md5Sum))

	storedItem, err := cSynced.Item(movedItem.Path)
	th.NokPrefix(t, err, "No such file")
	th.Equals(t, catalog.Item{}, storedItem)
	th.Equals(t, collectionClone, collectionCatalog)

}

func TestSyncStagingWhenFileNotInCollectionIsDeletedFromStaging(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	collectionClone := collectionCatalog.Clone()

	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	th.Ok(t, err)
	_, err = SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.Ok(t, err)

	deletedItem, err := catalog.NewItem(stagingFs, "subfolder/file1.bin")
	th.Ok(t, err)
	stagingFs.Remove(deletedItem.Path)

	cSynced, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.Ok(t, err)

	th.Equals(t, true, cSynced.IsDeletedChecksum(deletedItem.Md5Sum))
	th.Equals(t, true, cSynced.IsKnownChecksum(deletedItem.Md5Sum))

	storedItem, err := cSynced.Item(deletedItem.Path)
	th.NokPrefix(t, err, "No such file")
	th.Equals(t, catalog.Item{}, storedItem)
	th.Equals(t, collectionClone, collectionCatalog)
}

////////////////////////////////////

// func TestSyncStagingWhenNonDuplicateFileRemovedFromDisk(t *testing.T) {
// 	fs := createMemFsTestData()

// 	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
// 	th.Ok(t, err)
// 	_, err = SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	err = stagingFs.Remove("test1.txt")
// 	th.Ok(t, err)
// 	err = stagingFs.Remove("subfolder/file2.bin")
// 	th.Ok(t, err)

// 	cAfterDelete, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)
// 	th.Equals(t, 2, cAfterDelete.Count())
// 	th.Equals(t, 2, cAfterDelete.DeletedCount())
// 	checkFilesInCatalog(t, cAfterDelete, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cAfterDelete, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
// 	th.Equals(t, true, cAfterDelete.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117"))  // test1.txt
// 	th.Equals(t, true, cAfterDelete.IsDeletedChecksum("f350c40373648527aa95b15786473501"))  // subfolder/file2.bin
// 	th.Equals(t, false, cAfterDelete.IsDeletedChecksum("89b2b34c7b8d232041f0fcc1d213d7bc")) // test2.txt
// 	th.Equals(t, false, cAfterDelete.IsDeletedChecksum("1cb0bad847fb90f95a767854932ec7c4")) // subfolder/file1.bin
// }

// func TestSyncStagingWhenDuplicateFileRemovedFromDisk(t *testing.T) {
// 	fs := createMemFsTestData()

// 	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
// 	th.Ok(t, err)
// 	item, err := catalog.NewItem(stagingFs, "test1.txt")
// 	th.Ok(t, err)
// 	fsh.CopyFile(stagingFs, item.Path, item.ModificationTime, afero.NewBasePathFs(stagingFs, "subfolder"))

// 	cOrig, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	th.Equals(t, 5, cOrig.Count())
// 	th.Equals(t, 0, cOrig.DeletedCount())
// 	checkFilesInCatalog(t, cOrig, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cOrig, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
// 	checkFilesInCatalog(t, cOrig, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
// 	checkFilesInCatalog(t, cOrig, "subfolder/test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
// 	checkFilesInCatalog(t, cOrig, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")

// 	err = stagingFs.Remove("test1.txt")
// 	th.Ok(t, err)
// 	cAfterDelete, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Equals(t, 4, cAfterDelete.Count())
// 	th.Equals(t, 0, cAfterDelete.DeletedCount())
// 	checkFilesInCatalog(t, cAfterDelete, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cAfterDelete, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
// 	checkFilesInCatalog(t, cAfterDelete, "subfolder/test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
// 	checkFilesInCatalog(t, cAfterDelete, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")

// 	err = stagingFs.Remove("subfolder/test1.txt")
// 	th.Ok(t, err)
// 	cAfterDelete2, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Equals(t, 3, cAfterDelete2.Count())
// 	th.Equals(t, 1, cAfterDelete2.DeletedCount())
// 	checkFilesInCatalog(t, cAfterDelete2, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cAfterDelete2, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
// 	checkFilesInCatalog(t, cAfterDelete2, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
// 	th.Equals(t, true, cAfterDelete2.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117")) // test1.txt
// }

// func TestSyncStagingWhenFileAddedToDisk(t *testing.T) {
// 	fs := createMemFsTestData()

// 	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
// 	th.Ok(t, err)
// 	_, err = SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	dummy0 := dummies[0]
// 	createDummyFile(stagingFs, dummy0)

// 	cAfterAdd, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	th.Equals(t, 5, cAfterAdd.Count())
// 	th.Equals(t, 0, cAfterAdd.DeletedCount())
// 	checkFilesInCatalog(t, cAfterAdd, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cAfterAdd, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
// 	checkFilesInCatalog(t, cAfterAdd, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
// 	checkFilesInCatalog(t, cAfterAdd, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
// 	checkFilesInCatalog(t, cAfterAdd, dummy0.Path, dummy0.Size, dummy0.Md5Sum)
// }

// func TestSyncStagingWhenFileWithDeletedChecksumAddedToDisk(t *testing.T) {
// 	fs := createMemFsTestData()

// 	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
// 	th.Ok(t, err)
// 	cOrig, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	dummy0 := dummies[0]
// 	cOrig.DeleteChecksum(dummy0.Md5Sum)
// 	th.Equals(t, true, cOrig.IsDeletedChecksum(dummy0.Md5Sum))
// 	cOrig.Write(stagingFs, catalog.CatalogFileName)

// 	cRead, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)
// 	th.Equals(t, cOrig, cRead)

// 	err = createDummyFile(stagingFs, dummy0)
// 	th.Ok(t, err)
// 	cModified, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	th.Equals(t, 5, cModified.Count())
// 	th.Equals(t, 0, cModified.DeletedCount())
// 	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cModified, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
// 	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
// 	checkFilesInCatalog(t, cModified, dummy0.Path, dummy0.Size, dummy0.Md5Sum)
// 	th.Equals(t, false, cModified.IsDeletedChecksum(dummy0.Md5Sum))
// }

// func TestSyncStagingWhenFileModifiedOnDisk(t *testing.T) {
// 	fs := createMemFsTestData()

// 	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
// 	th.Ok(t, err)
// 	_, err = SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	// overwrite test1.txt with the dummy0
// 	dummy0 := dummies[0]
// 	dummy0.Path = "test1.txt"
// 	err = stagingFs.Remove("test1.txt")
// 	th.Ok(t, err)
// 	err = createDummyFile(stagingFs, dummy0)
// 	th.Ok(t, err)

// 	cModified, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	th.Equals(t, 4, cModified.Count())
// 	th.Equals(t, 0, cModified.DeletedCount())
// 	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cModified, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
// 	checkFilesInCatalog(t, cModified, "test1.txt", dummy0.Size, dummy0.Md5Sum)
// 	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
// }

// func TestSyncStagingWhenFileModifiedOnDiskToHaveADeletedCheckSum(t *testing.T) {
// 	fs := createMemFsTestData()

// 	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
// 	th.Ok(t, err)
// 	cOrig, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	// delete the checksum of dummy0 and save new catalog
// 	dummy0 := dummies[0]
// 	cOrig.DeleteChecksum(dummy0.Md5Sum)
// 	th.Equals(t, true, cOrig.IsDeletedChecksum(dummy0.Md5Sum))
// 	cOrig.Write(stagingFs, catalog.CatalogFileName)
// 	cRead, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)
// 	th.Equals(t, cOrig, cRead)

// 	// overwrite test1.txt with the dummy0
// 	dummy0.Path = "test1.txt"
// 	err = stagingFs.Remove("test1.txt")
// 	th.Ok(t, err)
// 	err = createDummyFile(stagingFs, dummy0)
// 	th.Ok(t, err)

// 	cModified, err := SyncCatalogWithStagingFolder(stagingFs)
// 	th.Ok(t, err)

// 	th.Equals(t, 4, cModified.Count())
// 	th.Equals(t, 0, cModified.DeletedCount())
// 	checkFilesInCatalog(t, cModified, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
// 	checkFilesInCatalog(t, cModified, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
// 	checkFilesInCatalog(t, cModified, "test1.txt", dummy0.Size, dummy0.Md5Sum)
// 	checkFilesInCatalog(t, cModified, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
// }
