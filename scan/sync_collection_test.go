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

