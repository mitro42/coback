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
	"github.com/pkg/errors"
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
func copyTestData(sourceFolder string, destinationFs afero.Fs) error {
	_, err := os.Stat("integration_test_data")
	if os.IsNotExist(err) {
		return errors.New("integration_test_data folder  doesn't exist. Please run generate_integration_test_data.sh")
	}
	sourceFs := fsh.CreateSafeFs("integration_test_data")
	return copyFolder(sourceFs, sourceFolder, destinationFs, sourceFolder)
}

// Copies a file between file systems, preserving the timestamps
func copyFileWithTimestamps(sourceFs afero.Fs, path string, destinationFs afero.Fs) error {
	info, err := sourceFs.Stat(path)
	if err != nil {
		return errors.Wrapf(err, "Failed to copy file: %s", path)
	}
	return fsh.CopyFile(sourceFs, path, info.ModTime().Format(time.RFC3339Nano), destinationFs)
}

// Copies a folder with all its contents between two filesystems
func copyFolder(sourceBaseFs afero.Fs, sourceFolder string, destinationBaseFs afero.Fs, destinationFolder string) error {
	sourceFs := newBasePathFs(sourceBaseFs, sourceFolder)
	destinationFs := newBasePathFs(destinationBaseFs, destinationFolder)
	afero.Walk(sourceFs, ".", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		err1 := fsh.CopyFile(sourceFs, path, info.ModTime().Format(time.RFC3339Nano), destinationFs)
		if err1 != nil {
			return errors.Wrapf(err1, "Failed to copy file: %s", path)
		}
		return nil
	})
	return nil
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
func prepareTestFs(t *testing.T, folders ...string) (afero.Fs, error) {
	fs := afero.NewMemMapFs()
	// fs := afero.NewBasePathFs(afero.NewOsFs(), "/tmp/xx")
	for _, folder := range folders {
		err := copyTestData(folder, fs)
		if err != nil {
			return nil, err
		}
	}
	return fs, nil
}

// Counts the number of files in a fs and fails the test is the actual number of
// the files differ from the expected number. Ignores files called coback.catalog.
func expectFileCount(t *testing.T, fs afero.Fs, expected int) {
	t.Helper()
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

// Checks that a file is NOT present in fs and fails the test it is present
func expectFileMissing(t *testing.T, fs afero.Fs, file string) {
	stat, err := fs.Stat(file)
	th.Equals(t, stat, nil)
	th.NokPrefix(t, err, "open")
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

func expectFolder3Contents(t *testing.T, fs afero.Fs, path string) {
	if path != "" {
		fs = afero.NewBasePathFs(fs, path)
	}
	expectFileCount(t, fs, 5)
	expectFile(t, fs, "family/mom.jpg")
	expectFile(t, fs, "family/sis.jpg")
	expectFile(t, fs, "friends/conor.jpg")
	expectFile(t, fs, "friends/markus.jpg")
	expectFile(t, fs, "funny.png")
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

	fs, err := prepareTestFs(t, "folder1", "folder2")
	th.Ok(t, err)
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

// NOTE This scenario is unfinished as it is not clear what the expected behavior should look  like here.
// In a sense this is a small user error. The files from the staging should be moved to the collection not copied.
// So whenever a new import is ran there should be no files present in staging that are already in the collection.
// But even if there are such files, the import should not fail completely (probably).
// Expected behavior TBD.
//
// func TestScenario1Copy(t *testing.T) {
// 	// Same as Scenario1 but all files are copied instead of moved.
// 	// A the end all files are deleted from staging and a reimport is performed.

// 	fs, err := prepareTestFs(t, "folder1", "folder2")
// 	th.Ok(t, err)
// 	import1Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder1", "staging", "collection")
// 	th.Ok(t, err)
// 	// 1
// 	err = run(import1Fs, stagingFs, collectionFs)
// 	th.Ok(t, err)
// 	expectFolder1Contents(t, import1Fs, ".")
// 	expectFolder1Contents(t, stagingFs, "1")

// 	// 2 (user action)
// 	copyFolder(stagingFs, "1", collectionFs, ".")
// 	expectFolder1Contents(t, stagingFs, "1")
// 	expectFolder1Contents(t, collectionFs, ".")

// 	// 3
// 	import2Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder2", "staging", "collection")
// 	err = run(import2Fs, stagingFs, collectionFs)
// 	th.Ok(t, err)
// 	expectFolder1Contents(t, collectionFs, ".")
// 	expectFolder2Contents(t, import2Fs, "")
// 	expectFileCount(t, stagingFs, 9)
// 	expectFile(t, stagingFs, "2/friends/tom.jpg")
// 	expectFile(t, stagingFs, "2/friends/jerry.jpg")

// 	// 4 (user action)
// 	copyFolder(stagingFs, "2", collectionFs, ".")
// 	expectFileCount(t, stagingFs, 9)

// 	// 5
// 	err = run(import1Fs, stagingFs, collectionFs)
// 	th.Ok(t, err)
// 	expectFileCount(t, stagingFs, 9)

// 	// 6
// 	err = run(import2Fs, stagingFs, collectionFs)
// 	th.Ok(t, err)
// 	expectFileCount(t, stagingFs, 9)

// 	// 7 - empty staging folder
// 	stagingFs.RemoveAll("1")
// 	stagingFs.RemoveAll("2")

// 	// 8 - test reimporting of folder1
// 	err = run(import1Fs, stagingFs, collectionFs)
// 	th.Ok(t, err)
// 	expectFileCount(t, stagingFs, 0)

// 	// 9 - test reimporting of folder2
// 	err = run(import2Fs, stagingFs, collectionFs)
// 	th.Ok(t, err)
// 	expectFileCount(t, stagingFs, 0)
// }

func TestScenario2(t *testing.T) {
	// All files copied to staging are deleted, nothing reaches the collection (coback.catalog is not deleted).
	// 1. Import folder1, check staging
	// 2. Delete all files from staging
	// 3. Import folder2, check staging
	// 4. Delete all files from staging
	// 5. Import folder1 again, check staging - must stay empty
	// 6. Import folder2 again, check staging - must stay empty

	fs, err := prepareTestFs(t, "folder1", "folder2")
	th.Ok(t, err)
	import1Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder1", "staging", "collection")
	th.Ok(t, err)
	// 1
	err = run(import1Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFolder1Contents(t, import1Fs, ".")
	expectFolder1Contents(t, stagingFs, "1")

	// 2 (user action)
	stagingFs.RemoveAll("1")
	expectFileCount(t, stagingFs, 0)

	// 3
	import2Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder2", "staging", "collection")
	err = run(import2Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFolder2Contents(t, import2Fs, "")
	expectFileCount(t, stagingFs, 2)
	expectFile(t, stagingFs, "1/friends/tom.jpg")
	expectFile(t, stagingFs, "1/friends/jerry.jpg")

	// 4 (user action)
	stagingFs.RemoveAll("1")
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

func TestScenario3(t *testing.T) {
	// Some files are moved from staging to collection, some files are deleted from staging, some files are deleted from collection later.
	// 1. Import folder1, check staging
	// 2. Move some files from staging to collection, delete the rest from staging
	// 3. Import folder2, check staging
	// 4. Move all files from staging to colletion
	// 5. Delete some files from collection that were moved there in step 1 or step 2
	// 6. Import folder1 again, check staging - must stay empty
	// 7. Import folder2 again, check staging - must stay empty
	// 8. Import folder3, check staging - must stay empty

	fs, err := prepareTestFs(t, "folder1", "folder2", "folder3")
	th.Ok(t, err)
	import1Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder1", "staging", "collection")
	th.Ok(t, err)
	// 1
	err = run(import1Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFolder1Contents(t, import1Fs, ".")
	expectFolder1Contents(t, stagingFs, "1")

	// 2 (user action)
	stagingFs.RemoveAll("1/family")
	moveFolder(stagingFs, "1/friends", collectionFs, ".")
	moveFolder(stagingFs, "1", collectionFs, "funny")
	expectFileCount(t, stagingFs, 0)

	// 3
	import2Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder2", "staging", "collection")
	err = run(import2Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFolder2Contents(t, import2Fs, "")
	expectFileCount(t, stagingFs, 2)
	expectFile(t, stagingFs, "1/friends/tom.jpg")
	expectFile(t, stagingFs, "1/friends/jerry.jpg")

	// 4 (user action)
	moveFolder(stagingFs, "1/friends", collectionFs, ".")
	expectFileCount(t, stagingFs, 0)

	// 5 (user action)
	collectionFs.Remove("funny/funny.png")
	collectionFs.Remove("funny/conor.jpg")
	collectionFs.Remove("funny/tom.jpg")

	// 6
	err = run(import1Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileCount(t, stagingFs, 0)

	// 7
	err = run(import2Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileCount(t, stagingFs, 0)

	// 8
	import3Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder3", "staging", "collection")
	err = run(import3Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileCount(t, stagingFs, 0)
}

func TestScenario4(t *testing.T) {
	// New files are added to the collection between import rounds that have not seen by CoBack before
	// and are not present in any catalog. Later some of these files are deleted then added again.
	// This scenario does not deal with duplicates (files with same content).
	// 1. Import folder3, check staging
	// 2. Move all files from staging to colletion (user action)
	// 3. Copy folder1/friends/kara.jpg and folder2/friends/tom.jpg to collection (user action)
	// 4. Import folder1, check staging - most not stage kara.jpg
	// 5. Delete tom.jpg from collection (user action)
	// 6. Import folder2, check staging - most not stage tom.jpg
	// 7. Copy folder2/friends/tom.jpg to collection (user action)
	// 8. Import folder2, check staging - most not stage tom.jpg

	fs, err := prepareTestFs(t, "folder1", "folder2", "folder3")
	th.Ok(t, err)
	import1Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder1", "staging", "collection")
	th.Ok(t, err)
	import2Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder2", "staging", "collection")
	th.Ok(t, err)
	import3Fs, stagingFs, collectionFs, err := initializeFolders(fs, "folder3", "staging", "collection")
	th.Ok(t, err)

	// 1
	err = run(import3Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFolder3Contents(t, stagingFs, "1")

	// 2 (user action)
	moveFolder(stagingFs, "1", collectionFs, ".")
	expectFileCount(t, stagingFs, 0)

	// 3 (user action)
	err = copyFileWithTimestamps(import1Fs, "friends/kara.jpg", collectionFs)
	th.Ok(t, err)
	err = copyFileWithTimestamps(import2Fs, "friends/tom.jpg", collectionFs)
	th.Ok(t, err)

	// 4
	err = run(import1Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileMissing(t, stagingFs, "1/friends/kara.jpg")

	// 5 (user action)
	err = collectionFs.Remove("friends/tom.jpg")
	th.Ok(t, err)

	// 6
	err = run(import2Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileMissing(t, stagingFs, "2/friends/tom.jpg")

	// 7 (user action)
	err = copyFileWithTimestamps(import2Fs, "friends/tom.jpg", collectionFs)
	th.Ok(t, err)

	// 8
	err = run(import2Fs, stagingFs, collectionFs)
	th.Ok(t, err)
	expectFileMissing(t, stagingFs, "2/friends/tom.jpg")
}

// New files are added to the staging between import rounds, that have not seen by CoBack before and are not present in any catalog.
// Later some of these files are deleted. (user error)

// File edited, to have new unique content while keeping the same name.
// edited in collection
// - file is overwritten with new content (white balance change, old image is discarded)
//	 ---> old image marked as deleted, new image added as regular new file
// - new file is created in collection based on an already present image (watermark, original file is kept,
//	 cannot detect the connection it's simply a new file)
//   ---> no change for the original file, new added image added as regular new file
// - old file is renamed, file with the original name is overwritten with new content (e.g. holiday123.jpg is cropped, original file kept as holiday123_orig.jpg)
//   ---> original hash is kept, new hash is added, path to hash map is updated accordingly

// edited in staging
// 1. Edit happens in staging, and left there until coback is ran again
// - file is overwritten with new content (white balance change, old image is discarded)
//	 ---> old image treated as is it was deleted, new image added as regular new file (??)
// - new file is created in collection based on an already present image (watermark, original file is kept,
//	 cannot detect the connection it's simply a new file)
//   ---> no change for the original file, new added image added as regular new file
// - old file is renamed, file with the original name is overwritten with new content (e.g. holiday123.jpg is cropped, original file kept as holiday123_orig.jpg)
//   ---> original hash is kept, new hash is added, path to hash map is updated accordingly
// 2. Edit happens in staging, file is moved to collection before coback is ran again
// - file is overwritten with new content (white balance change, old image is discarded)
//	 ---> old image marked as deleted, new image added as regular new file
// - new file is created in collection based on an already present image (watermark, original file is kept,
//	 cannot detect the connection it's simply a new file)
//   ---> no change for the original file, new added image added as regular new file
// - old file is renamed, file with the original name is overwritten with new content (e.g. holiday123.jpg is cropped, original file kept as holiday123_orig.jpg)
//   ---> original hash is kept, new hash is added, path to hash map is updated accordingly
// 3. Edit happens in staging, file is deleted before coback is ran again
//   ---> doesn't matter (cannot detect either) it is the same as if the file was deleted without any change

// edited in import - N/A

// Import folder has duplicates (both different folder/same name and same folder/different name)

// Starting from non-empty collection

// Starting from non-empty staging

// Quick scans

// Forced deep scans (?)
