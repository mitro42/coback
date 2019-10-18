package main

import (
	"fmt"
	"os"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	"github.com/spf13/afero"
)

// updateCatalog reads a catalog file in the base of the FS, checks it against the contents of the FS
// and updates it as necessary. The returned Catalog is always consistent with the FS.
// The result is undefined if the FS is changed by other processes.
// func updateCatalog(fs afero.Fs, name string) (catalog.Catalog, error) {
// 	fmt.Println("Reading catalog")
// 	c, err := catalog.Read(fs, catalog.CatalogFileName)
// 	if err != nil {
// 		fmt.Println("Cannot read catalog. Folder must be rescanned...")
// 		c = catalog.Scan(fs)
// 		return c, nil
// 	}

// 	fmt.Println("Comparing folder contents with catalog")
// 	diff := catalog.Diff(fs, c)
// 	if len(diff.Update) > 0 {
// 		fmt.Println("Some file have changed. Folder must be rescanned...")
// 		c = catalog.Scan(fs)
// 	} else if len(diff.Add) > 0 {
// 		fmt.Println("Some files have been added to the folder. Adding them to the catalog...")
// 		c = catalog.ScanAdd(fs, c, diff)
// 	} else {
// 		fmt.Println("The catalog is up to date")
// 	}

// 	return c, nil
// }

func readAndDiffCatalog(fs afero.Fs, name string) (catalog.Catalog, catalog.FileSystemDiff, error) {
	fmt.Println("Reading catalog")
	c, err := catalog.Read(fs, catalog.CatalogFileName)
	if err != nil {
		fmt.Println("Cannot read catalog. Folder must be rescanned...")
		c = catalog.Scan(fs)
		return c, catalog.NewFileSystemDiff(), nil
	}
	fmt.Println("Comparing folder contents with catalog")
	diff := catalog.Diff(fs, c)
	return c, diff, nil
}

// initializeFolder prepares a folder to be used by CoBack.
// Creates the folder if necessary and returns an afero.Fs which roots at the specified folder.
func initializeFolder(baseFs afero.Fs, path string, name string) (afero.Fs, error) {
	err := fsh.EnsureDirectoryExist(baseFs, path)
	if err != nil {
		return nil, err
	}
	fs := afero.NewBasePathFs(baseFs, path)
	return fs, nil
}

// syncCatalogWithImportFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the import folder.
func syncCatalogWithImportFolder(fs afero.Fs) (catalog.Catalog, error) {
	fmt.Println("***************** Processing import folder ***************")
	c, _, err := readAndDiffCatalog(fs, "import")
	if err != nil {
		return nil, err
	}

	return c, nil
}

// syncCatalogWithStagingFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the staging folder.
func syncCatalogWithStagingFolder(fs afero.Fs) (catalog.Catalog, error) {
	fmt.Println("***************** Processing staging folder ***************")
	c, _, err := readAndDiffCatalog(fs, "staging")
	if err != nil {
		return nil, err
	}

	return c, nil
}

// syncCatalogWithCollectionFolder makes sure that the catalog in the folder is in sync with the file system
// The fs parameter is treated as the root of the Collection folder.
func syncCatalogWithCollectionFolder(fs afero.Fs) (catalog.Catalog, error) {
	fmt.Println("***************** Processing collection folder ***************")
	c, _, err := readAndDiffCatalog(fs, "collection")
	if err != nil {
		return nil, err
	}

	return c, nil
}

// stageFiles creates a new numbered folder in the staging folder and copies all files
// in the items channel from the import FS to this new folder in the staging FS.
func stageFiles(importFs afero.Fs, items <-chan catalog.Item, stagingFs afero.Fs) error {
	fmt.Println("***************** Copying files to staging folder *****************")
	targetFolder := fsh.NextUnusedFolder(stagingFs)
	targetFs := afero.NewBasePathFs(stagingFs, targetFolder)
	for item := range items {
		fmt.Println(item.Path)
		err := fsh.CopyFile(importFs, item.Path, item.ModificationTime, targetFs)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("Usage: %v import-from-path staging-path collection-path\n", os.Args[0])
		os.Exit(-1)
	}
	baseFs := afero.NewOsFs()

	importFs, err := initializeFolder(baseFs, os.Args[1], "Import")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	importCatalog, err := syncCatalogWithImportFolder(importFs)
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}

	stagingFs, err := initializeFolder(baseFs, os.Args[2], "Staging")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	stagingCatalog, err := syncCatalogWithStagingFolder(stagingFs)
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}

	collectionFs, err := initializeFolder(baseFs, os.Args[3], "Collection")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	collectionCatalog, err := syncCatalogWithCollectionFolder(collectionFs)
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}

	notInCollection := importCatalog.FilterNew(collectionCatalog)
	notInStaging := notInCollection.FilterNew(stagingCatalog)

	if err = stageFiles(importFs, notInStaging.AllItems(), stagingFs); err != nil {
		fmt.Printf("Failed to copy files: %v\n", err)
	}
}
