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
