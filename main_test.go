package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
	// "github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

// General notes:
// These tests cover complex end to end use cases. All of them contain multiple rounds of imports.
// During all tests the import folders must not change, and the collection folder cannot be changed by run(), only by steps emulating user actions.
// Unless otherwise stated, all scenarios start from an empty collection and empty staging folder.
// At the end of each scenario all the import folders must be reimported and the staging must stay empty.
// Then catalogs in the import folders must be deleted and before doing another reimport of the folders.

// Test data description
// Files are listed as "filename.ext[id]", the id is not part of the actual file name,
// but the files with the same id has the same content. The files have some random content.
// folder1
//  |- family
//  |    |- mom.jpg[1]
//  |    |- dad.jpg[2]
//  |    |- sis.jpg[3]
//  |- friends
//  |    |- kara.jpg[4]
//  |    |- conor.jpg[5]
//  |    |- markus.jpg[6]
//  |- funny.png[7]

// folder2 - has partial overlap with folder1, one file has different name but same content
//  |- family
//  |    |- mom.jpg[1]
//  |    |- daddy.jpg[2]
//  |- friends
//  |    |- tom.jpg[8]
//  |    |- jerry.jpg[9]
//  |    |- markus.jpg[6]

// folder3 - subset of folder1
//  |- family
//  |    |- mom.jpg[1]
//  |    |- sis.jpg[3]
//  |- friends
//  |    |- conor.jpg[5]
//  |    |- markus.jpg[6]
//  |- funny.png[7]

// folder4 - has duplicates in itself
//  |- holiday
//  |    |- public
//  |    |    |- view1.jpg[10]
//  |    |    |- view2.jpg[11]
//  |    |- view1.jpg[10]
//  |    |- view2.jpg[11]
//  |    |- view3.jpg[12]
//  |- view1.jpg[10]

// Copies one folder of the pre-generated test data to the specified fs.
func copyFolder(t *testing.T, fs afero.Fs, folder string) {
	_, err := os.Stat("integration_test_data")
	if os.IsNotExist(err) {
		t.Fatalf("integration_test_data folder  doesn't exist. Please run generate_integration_test_data.sh")
	}
	sourceFs := fsh.CreateSafeFs("integration_test_data")
	afero.Walk(sourceFs, folder, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		fsh.CopyFile(sourceFs, path, info.ModTime().Format(time.RFC3339Nano), fs)
		return nil
	})
}

func newBasePathFs(fs afero.Fs, folder string) afero.Fs {
	if folder == "" || folder == "." {
		return fs
	}

	return afero.NewBasePathFs(fs, folder)
}

// Moves the contents of a folder from sourceFs to destinationFs.
// The file modification times and attributes are preserved.
func moveFolder(sourceBaseFs afero.Fs, sourceFolder string, destinationBaseFs afero.Fs, destinationFolder string) error {
	sourceFs := newBasePathFs(sourceBaseFs, sourceFolder)
	destinationFs := newBasePathFs(destinationBaseFs, destinationFolder)
	afero.Walk(sourceFs, ".", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		fsh.CopyFile(sourceFs, path, info.ModTime().Format(time.RFC3339Nano), destinationFs)
		err1 := sourceFs.Remove(path)
		if err1 != nil {
			return err1
		}
		return nil
	})

	return sourceBaseFs.RemoveAll(sourceFolder)
}

// Creates a MemoryFs and copies a set of folders of pre-generated test data into it.
// The returned fs can be used as a source of imports in integration tests.
func prepareTestFs(t *testing.T, folders ...string) afero.Fs {
	fs := afero.NewMemMapFs()
	// fs := afero.NewBasePathFs(afero.NewOsFs(), "/tmp/xx")
	for _, folder := range folders {
		copyFolder(t, fs, folder)
	}
	return fs
}

// Counts the number of files in a fs and fails the test is the actual number of
// the files differ from the expected number. Ignores files called coback.catalog.
func expectFileCount(t *testing.T, fs afero.Fs, expected int) {
	actual := 0
	afero.Walk(fs, ".", func(path string, info os.FileInfo, err error) error {
		// fmt.Printf("expectFileCount info: %v\n", info)
		// fmt.Printf("expectFileCount err: %v\n", err)
		// fmt.Printf("expectFileCount path: %v\n", path)
		// fmt.Printf("err: %v\n", err)
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, "coback.catalog") {
			return nil
		}
		actual++
		return nil
	})
	th.Equals(t, expected, actual)
}

// Checks that a file is present in fs and fails the test if not.
func expectFile(t *testing.T, fs afero.Fs, file string) {
	stat, err := fs.Stat(file)
	th.Ok(t, err)
	th.Equals(t, false, stat.IsDir())
}

func expectFolder1Contents(t *testing.T, fs afero.Fs, path string) {
	if path != "." {
		fs = afero.NewBasePathFs(fs, path)
	}

	expectFileCount(t, fs, 7)
	expectFile(t, fs, "family/dad.jpg")
	expectFile(t, fs, "family/mom.jpg")
	expectFile(t, fs, "family/sis.jpg")
	expectFile(t, fs, "friends/conor.jpg")
	expectFile(t, fs, "friends/kara.jpg")
	expectFile(t, fs, "friends/markus.jpg")
	expectFile(t, fs, "funny.png")
}

func expectFolder2Contents(t *testing.T, fs afero.Fs, path string) {
	if path != "" {
		fs = afero.NewBasePathFs(fs, path)
	}
	expectFileCount(t, fs, 5)
	expectFile(t, fs, "family/daddy.jpg")
	expectFile(t, fs, "family/mom.jpg")
	expectFile(t, fs, "friends/jerry.jpg")
	expectFile(t, fs, "friends/markus.jpg")
	expectFile(t, fs, "friends/tom.jpg")
}

func listFiles(label string, fs afero.Fs, folder string) {
	afero.Walk(fs, folder, func(path string, info os.FileInfo, err error) error {
		fmt.Printf("%s: %s %v\n", label, path, info.IsDir())
		return nil
	})
}

func TestScenario1(t *testing.T) {
	// Simple use case, multiple rounds of import with reimporting already seen files.
	// Each of the following cases are present:
	// - a file is moved to an identical relative folder structure and file name
	// - a file is moved to an identical relative folder structure but with different file name
	// - a file is moved to a different folder but with the original name
	// - a file is moved to a different folder with different name
	// 1. Import folder1, check staging
	// 2. Move all files from staging to colletion (user action)
	// 3. Import folder2, check staging
	// 4. Move all files from staging to collection (user action)
	// 5. Import folder1 again, check staging - must stay empty
	// 6. Import folder2 again, check staging - must stay empty

	fs := prepareTestFs(t, "folder1", "folder2")
	import1Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder1", "staging", "collection")
	th.Ok(t, err)
	// 1
	err = run(import1Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFolder1Contents(t, import1Fs, ".")
	expectFolder1Contents(t, stagingFs, "1")

	// 2 (user action)
	moveFolder(stagingFs, "1", collectionFs, ".")
	expectFolder1Contents(t, collectionFs, ".")

	// 3
	import2Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder2", "staging", "collection")
	err = run(import2Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFolder1Contents(t, collectionFs, ".")
	expectFolder2Contents(t, import2Fs, "")
	expectFileCount(t, stagingFs, 2)
	expectFile(t, stagingFs, "1/friends/tom.jpg")
	expectFile(t, stagingFs, "1/friends/jerry.jpg")

	// 4 (user action)
	moveFolder(stagingFs, "1", collectionFs, ".")
	expectFileCount(t, stagingFs, 0)

	// 5
	err = run(import1Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileCount(t, stagingFs, 0)

	// 6
	err = run(import2Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileCount(t, stagingFs, 0)
}

func TestScenario1Copy(t *testing.T) {
	// Same as Scenario1 but all files are copied instead of moved.
	// A the end all files are deleted from staging and a reimport is performed.
}

func TestScenario2(t *testing.T) {
	// All files copied to staging are deleted, nothing reaches the collection.
	// 1. Import folder1, check staging
	// 2. Delete all files from staging
	// 3. Import folder2, check staging
	// 4. Delete all files from staging
	// 5. Import folder1 again, check staging - must stay empty
	// 6. Import folder2 again, check staging - must stay empty
}

func TestScenario3(t *testing.T) {
	// Some files are moved from staging to collection, some files are deleted from staging, some files are deleted from collection later.
	// 1. Import folder1, check staging
	// 2. Move all files from staging to colletion
	// 3. Import folder2, check staging
	// 4. Move some files from staging to collection
	// 5. Delete some files from collection that were moved there in step 2
	// 6. Import folder1 again, check staging - must stay empty
	// 7. Import folder2 again, check staging - must stay empty
}

// File edited, to have new unique content while keeping the same name.

// New files are added to the collection between import rounds, that have not seen by CoBack before and are not present in any catalog.
// Later some of these files are deleted.

// Quick scans

// Forced deep scans (?)

// Import folder has duplicates (both different folder/same name and same folder/different name)
