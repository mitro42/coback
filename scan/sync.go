package scan

import (
	"fmt"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	"github.com/spf13/afero"
)

// InitializeFolder prepares a folder to be used by CoBack.
// Creates the folder if necessary and returns an afero.Fs which roots at the specified folder.
func InitializeFolder(baseFs afero.Fs, path string, name string) (afero.Fs, error) {
	err := fsh.EnsureDirectoryExist(baseFs, path)
	if err != nil {
		return nil, err
	}
	fs := afero.NewBasePathFs(baseFs, path)
	return fs, nil
}

func readAndDiffCatalog(fs afero.Fs, name string) (catalog.Catalog, FileSystemDiff, error) {
	fmt.Println("Reading catalog")
	c, err := catalog.Read(fs, catalog.CatalogFileName)
	if err != nil {
		fmt.Println("Cannot read catalog. Folder must be rescanned...")
		c = Scan(fs)
		return c, NewFileSystemDiff(), nil
	}
	fmt.Println("Comparing folder contents with catalog")
	diff := Diff(fs, c)
	return c, diff, nil
}

// SyncCatalogWithImportFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the import folder.
func SyncCatalogWithImportFolder(fs afero.Fs) (catalog.Catalog, error) {
	fmt.Println("***************** Processing import folder ***************")
	c, diff, err := readAndDiffCatalog(fs, "import")
	if err != nil {
		return nil, err
	}

	if len(diff.Delete) > 0 {
		c = Scan(fs)
	} else if len(diff.Add) > 0 {
		c = ScanAdd(fs, c, diff)
	}

	return c, nil
}

// SyncCatalogWithStagingFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the staging folder.
func SyncCatalogWithStagingFolder(fs afero.Fs) (catalog.Catalog, error) {
	fmt.Println("***************** Processing staging folder ***************")
	c, _, err := readAndDiffCatalog(fs, "staging")
	if err != nil {
		return nil, err
	}

	return c, nil
}

// SyncCatalogWithCollectionFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the Collection folder.
func SyncCatalogWithCollectionFolder(fs afero.Fs) (catalog.Catalog, error) {
	fmt.Println("***************** Processing collection folder ***************")
	c, _, err := readAndDiffCatalog(fs, "collection")
	if err != nil {
		return nil, err
	}

	return c, nil
}
