package scan

import (
	"fmt"
	"os"
	"path/filepath"
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

	th.Equals(t, c.Count(), 4)
	th.Equals(t, c.DeletedCount(), 0)
	fmt.Printf("%v\n", c)
	checkFilesInCatalog(t, c, "subfolder/file1.bin", 1024, "1cb0bad847fb90f95a767854932ec7c4")
	checkFilesInCatalog(t, c, "subfolder/file2.bin", 1500, "f350c40373648527aa95b15786473501")
	checkFilesInCatalog(t, c, "test1.txt", 1160, "b3cd1cf6179bca32fd5d76473b129117")
	checkFilesInCatalog(t, c, "test2.txt", 1304, "89b2b34c7b8d232041f0fcc1d213d7bc")
}
