package main

import (
	"fmt"
	"os"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	"github.com/mitro42/coback/scan"
	"github.com/spf13/afero"
)

// stageFiles creates a new numbered folder in the staging folder and copies all files
// in the items channel from the import FS to this new folder in the staging FS.
func stageFiles(importFs afero.Fs, items <-chan catalog.Item, stagingFs afero.Fs) error {
	fmt.Println("***************** Copying files to staging folder *****************")
	targetFolder := fsh.NextUnusedFolder(stagingFs)
	fsh.EnsureDirectoryExist(stagingFs, targetFolder)
	targetFs := afero.NewBasePathFs(stagingFs, targetFolder)
	for item := range items {
		if item.Path == "" {
			return nil
		}
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

	importFs, err := scan.InitializeFolder(baseFs, os.Args[1], "Import")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	importCatalog, err := scan.SyncCatalogWithImportFolder(importFs)
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	importCatalog.Write(importFs)

	collectionFs, err := scan.InitializeFolder(baseFs, os.Args[3], "Collection")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	collectionCatalog, err := scan.SyncCatalogWithCollectionFolder(collectionFs)
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}

	stagingFs, err := scan.InitializeFolder(baseFs, os.Args[2], "Staging")
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}
	stagingCatalog, err := scan.SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(-2)
	}

	for deletedChecksum := range stagingCatalog.DeletedChecksums() {
		collectionCatalog.DeleteChecksum(deletedChecksum)
		stagingCatalog.UnDeleteChecksum(deletedChecksum)
	}
	collectionCatalog.Write(collectionFs)
	stagingCatalog.Write(stagingFs)

	notInCollection := importCatalog.FilterNew(collectionCatalog)
	notInStaging := notInCollection.FilterNew(stagingCatalog)

	if err = stageFiles(importFs, notInStaging.AllItems(), stagingFs); err != nil {
		fmt.Printf("Failed to copy files: %v\n", err)
	}
}
