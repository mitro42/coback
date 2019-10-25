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
	cSynced.Write(stagingFs)
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

func TestSyncStaginNewFileThatIsAlreadyInCollection(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	cOrig := Scan(stagingFs)
	dummy0 := dummies[0]
	createDummyFile(stagingFs, dummy0)

	item, err := catalog.NewItem(stagingFs, dummy0.Path)
	th.Ok(t, err)
	collectionCatalog.Add(*item)

	c, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.NokPrefix(t, err, "File is already in the collection")
	th.Equals(t, nil, c)

	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)
}

func TestSyncStaginNewFileThatIsAlreadyDeletedInCollection(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	cOrig := Scan(stagingFs)
	dummy0 := dummies[0]
	createDummyFile(stagingFs, dummy0)

	collectionCatalog.DeleteChecksum(dummy0.Md5Sum)

	c, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.NokPrefix(t, err, "File is already deleted from the collection")
	th.Equals(t, nil, c)

	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)
}

func TestSyncStaginNewFileThatIsAlreadyInStaging(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	cOrig := Scan(stagingFs)

	item, err := catalog.NewItem(stagingFs, "test1.txt")
	th.Ok(t, err)
	fsh.CopyFile(stagingFs, item.Path, item.ModificationTime, afero.NewBasePathFs(stagingFs, "subfolder"))

	c, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.NokPrefix(t, err, "File is already in the staging folder")
	th.Equals(t, nil, c)

	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)
}

func TestSyncStaginNewFileThatIsAlreadyDeletedInStaging(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	cOrig := Scan(stagingFs)
	dummy0 := dummies[0]
	createDummyFile(stagingFs, dummy0)

	cOrig.DeleteChecksum(dummy0.Md5Sum)
	cOrig.Write(stagingFs)

	cSynced, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.NokPrefix(t, err, "File is already deleted from the staging folder")
	th.Equals(t, nil, cSynced)

	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)
}

func TestSyncStaginNewFileThatIsNotKnownInCollectionOrStaging(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	cOrig := Scan(stagingFs)
	dummy0 := dummies[0]
	createDummyFile(stagingFs, dummy0)

	cSynced, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.Ok(t, err)
	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)
	item, err := catalog.NewItem(stagingFs, dummy0.Path)
	th.Ok(t, err)
	cOrig.Add(*item)
	th.Equals(t, cOrig, cSynced)
}

func TestSyncStaginFileChanged(t *testing.T) {
	collectionCatalog := catalog.NewCatalog()
	fs := createMemFsTestData()
	stagingFs, err := InitializeFolder(fs, "test_data", "Staging")
	cOrig := Scan(stagingFs)

	item, err := catalog.NewItem(stagingFs, "test1.txt")
	th.Ok(t, err)
	item.Md5Sum = "42"
	cOrig.Set(*item)
	cOrig.Write(stagingFs)

	cSynced, err := SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	th.NokPrefix(t, err, "A file already in the staging folder has been modified")
	th.Equals(t, nil, cSynced)
	cRead, err := catalog.Read(stagingFs, catalog.CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, cOrig, cRead)
}
