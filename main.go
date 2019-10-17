package main

import (
	"fmt"
	"os"

	"github.com/mitro42/coback/catalog"
	"github.com/spf13/afero"
)

// updateCatalog reads a catalog file in the base of the FS, checks it against the contents of the FS
// and updates it as necessary. The returned Catalog is always consistent with the FS.
// The result is undefined if the FS is changed by other processes.
func updateCatalog(fs afero.Fs, name string) (catalog.Catalog, error) {
	fmt.Println("Reading catalog")
	c, err := catalog.Read(fs, catalog.CatalogFileName)
	if err != nil {
		fmt.Println("Cannot read catalog. Folder must be rescanned...")
		c = catalog.Scan(fs)
		return c, nil
	}

	fmt.Println("Comparing folder contents with catalog")
	diff := catalog.Diff(fs, c)
	if len(diff.Update) > 0 {
		fmt.Println("Some file have changed. Folder must be rescanned...")
		c = catalog.Scan(fs)
	} else if len(diff.Add) > 0 {
		fmt.Println("Some files have been added to the folder. Adding them to the catalog...")
		c = catalog.ScanAdd(fs, c, diff)
	} else {
		fmt.Println("The catalog is up to date")
	}

	return c, nil
}

// initializeFolder prepares a folder to be used by CoBack.
// If the folder doesn't exist the function creates it and creates an empty catalog in it.
// If the folder exists but there's no catalog in it, it performs a full scan.
// If there is a catalog already in the folder, the function checks the contents and updates the catalog
// as necessary. In any case when the function returns without an error the folder will exist
// and will have an up to date catalog in it.
func initializeFolder(path string, name string) (afero.Fs, catalog.Catalog, error) {
	fmt.Printf("***************** Processing %v folder ***************\n", name)
	baseFs := afero.NewOsFs()
	err := ensureDirectoryExist(baseFs, path)
	if err != nil {
		return nil, nil, err
	}
	fs := afero.NewBasePathFs(baseFs, path)
	c, err := updateCatalog(fs, name)
	if err != nil {
		return nil, nil, err
	}

	return fs, c, nil
}

// stageFiles creates a new numbered folder in the staging folder and copies all files
// in the items channel from the import FS to this new folder in the staging FS.
func stageFiles(importFs afero.Fs, items <-chan catalog.Item, stagingFs afero.Fs) error {
	fmt.Println("***************** Copying files to staging folder *****************")
	targetFolder := nextUnusedFolder(stagingFs)
	targetFs := afero.NewBasePathFs(stagingFs, targetFolder)
	for item := range items {
		fmt.Println(item.Path)
		err := copyFile(importFs, item, targetFs)
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

	importFs, importCatalog, err := initializeFolder(os.Args[1], "Import")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	stagingFs, stagingCatalog, err := initializeFolder(os.Args[2], "Staging")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	_, collectionCatalog, err := initializeFolder(os.Args[3], "Collection")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}

	notInCollection := importCatalog.FilterNew(collectionCatalog)
	notInStaging := notInCollection.FilterNew(stagingCatalog)

	if err = stageFiles(importFs, notInStaging.AllItems(), stagingFs); err != nil {
		fmt.Printf("Failed to copy files: %v\n", err)
	}
	fmt.Println(importCatalog.Count())
	fmt.Println(stagingCatalog.Count())
	fmt.Println(collectionCatalog.Count())
}
