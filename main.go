package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	"github.com/mitro42/coback/scan"
	"github.com/pkg/errors"
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
		fmt.Printf("%s --> %s\n", item.Path, filepath.Join(targetFolder, item.Path))
		err := fsh.CopyFile(importFs, item.Path, item.ModificationTime, targetFs)
		if err != nil {
			return err
		}
	}
	return nil
}

func initializeFolders(baseFs afero.Fs, fromPath string, stagingPath string, toPath string) (importFs afero.Fs, stagingFs afero.Fs, collectionFs afero.Fs, err error) {
	importFs, err = scan.InitializeFolder(baseFs, fromPath, "Import")
	if err != nil {
		return nil, nil, nil, err
	}

	stagingFs, err = scan.InitializeFolder(baseFs, stagingPath, "Staging")
	if err != nil {
		return nil, nil, nil, err
	}

	collectionFs, err = scan.InitializeFolder(baseFs, toPath, "Collection")
	if err != nil {
		return nil, nil, nil, err
	}
	return
}

func run(importFs afero.Fs, stagingFs afero.Fs, collectionFs afero.Fs) error {
	importCatalog, err := scan.SyncCatalogWithImportFolder(importFs)
	if err != nil {
		return errors.Wrapf(err, "Cannot sync folder contents")
	}
	importCatalog.Write(importFs)

	collectionCatalog, err := scan.SyncCatalogWithCollectionFolder(collectionFs)
	if err != nil {
		return errors.Wrapf(err, "Cannot sync folder contents")
	}

	stagingCatalog, err := scan.SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	if err != nil {
		return errors.Wrapf(err, "Cannot sync folder contents")
	}

	for deletedChecksum := range stagingCatalog.DeletedChecksums() {
		collectionCatalog.DeleteChecksum(deletedChecksum)
		stagingCatalog.UnDeleteChecksum(deletedChecksum)
	}
	collectionCatalog.Write(collectionFs)

	notInCollection := importCatalog.FilterNew(collectionCatalog)
	notInStaging := notInCollection.FilterNew(stagingCatalog)

	if err = stageFiles(importFs, notInStaging.AllItems(), stagingFs); err != nil {
		return errors.Wrapf(err, "Failed to copy files")
	}

	stagingCatalog, err = scan.SyncCatalogWithStagingFolder(stagingFs, collectionCatalog)
	if err != nil {
		return errors.Wrapf(err, "Cannot sync folder contents after staging")
	}
	stagingCatalog.Write(stagingFs)
	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("Usage: %v import-from-path staging-path collection-path\n", os.Args[0])
		os.Exit(1)
	}
	baseFs := afero.NewOsFs()

	importFs, stagingFs, collectionFs, err := initializeFolders(baseFs, os.Args[0], os.Args[1], os.Args[2])
	if err != nil {
		fmt.Printf("Cannot initialize folder: %v\n", err)
		os.Exit(1)
	}

	err = run(importFs, stagingFs, collectionFs)
	if err != nil {
		fmt.Printf("Failed to copy files: %v\n", err)
		os.Exit(1)
	}

}
