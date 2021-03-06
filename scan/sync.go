package scan

import (
	"fmt"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// InitializeFolder prepares a folder to be used by CoBack.
// Creates the folder if necessary and returns an afero.Fs which roots at the specified folder.
func InitializeFolder(baseFs afero.Fs, path string) (afero.Fs, error) {
	if path == "." {
		return baseFs, nil
	}
	err := fsh.EnsureDirectoryExist(baseFs, path)
	if err != nil {
		return nil, err
	}
	fs := afero.NewBasePathFs(baseFs, path)
	return fs, nil
}

// readAndDiffCatalog attempts to read a catalog. If the catalog is present, it diffs the contents with the file system.
// If the catalog is missing a full scan is performed and an empty diff is returned.
func readAndDiffCatalog(fs afero.Fs, name string) (catalog.Catalog, FileSystemDiff, error) {
	fmt.Println("Reading catalog")
	c, err := catalog.Read(fs, catalog.CatalogFileName)
	if err != nil {
		fmt.Println("Cannot read catalog. Folder must be rescanned...")
		c = Scan(fs)
		return c, NewFileSystemDiff(), nil
	}
	fmt.Println("Comparing folder contents with catalog")
	diff := Diff(fs, c, false)
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

	if len(diff.Delete) > 0 || len(diff.Update) > 0 {
		c = Scan(fs)
	} else if len(diff.Add) > 0 {
		c = ScanAdd(fs, c, diff)
	}

	return c, nil
}

// SyncCatalogWithStagingFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the staging folder.
func SyncCatalogWithStagingFolder(fs afero.Fs, collection catalog.Catalog) (catalog.Catalog, error) {
	fmt.Println("***************** Processing staging folder ***************")
	c, diff, err := readAndDiffCatalog(fs, "staging")
	if err != nil {
		return nil, err
	}

	for deletedPath := range diff.Delete {
		item, err := c.Item(deletedPath)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to remove deleted file")
		}
		if collection.IsKnownChecksum(item.Md5Sum) {
			c.ForgetPath(item.Path)
		}
		c.DeletePath(deletedPath)
	}

	for addedPath := range diff.Add {
		item, err := catalog.NewItem(fs, addedPath)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to check new file")
		}
		if collection.IsDeletedChecksum(item.Md5Sum) {
			return nil, fmt.Errorf("File is already deleted from the collection: %v", item.Path)
		}
		if collection.IsKnownChecksum(item.Md5Sum) {
			return nil, fmt.Errorf("File is already in the collection: %v", item.Path)
		}
		if c.IsDeletedChecksum(item.Md5Sum) {
			return nil, fmt.Errorf("File is already deleted from the staging folder: %v", item.Path)
		}
		c.Add(*item)
	}

	for addedPath := range diff.Update { // in later versions all modified files will be returned in some form
		return nil, fmt.Errorf("A file already in the staging folder has been modified: %v", addedPath)
	}

	for item := range c.AllItems() {
		if collection.IsDeletedChecksum(item.Md5Sum) {
			fs.Remove(catalog.CatalogFileName)
			return nil, fmt.Errorf("File is already deleted from the collection: %v", item.Path)
		}
		if collection.IsKnownChecksum(item.Md5Sum) {
			fs.Remove(catalog.CatalogFileName)
			return nil, fmt.Errorf("File is already in the collection: %v", item.Path)
		}
	}

	return c, nil
}

// SyncCatalogWithCollectionFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the Collection folder.
func SyncCatalogWithCollectionFolder(fs afero.Fs) (catalog.Catalog, error) {
	fmt.Println("***************** Processing collection folder ***************")
	c, diff, err := readAndDiffCatalog(fs, "collection")
	if err != nil {
		return nil, err
	}

	for deletedPath := range diff.Delete {
		c.DeletePath(deletedPath)
	}

	for addedPath := range diff.Add {
		item, err := catalog.NewItem(fs, addedPath)
		if err != nil {
			return nil, err
		}
		err = c.Add(*item)
		if err != nil {
			return nil, err
		}
	}

	for modifiedPath := range diff.Update {
		item, err := catalog.NewItem(fs, modifiedPath)
		if err != nil {
			return nil, err
		}
		err = c.Set(*item)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}
