package main

import (
	// "os"
	// "path/filepath"
	"testing"
	// "time"
	// "github.com/mitro42/coback/catalog"
	// fsh "github.com/mitro42/coback/fshelper"
	// th "github.com/mitro42/testhelper"
	// "github.com/spf13/afero"
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

func TestScenario1(t *testing.T) {
	// Simple use case, multiple rounds of import with reimporting already seen files.
	// Each of the following cases are present:
	// - a file is moved to an identical relative folder structure and file name
	// - a file is moved to an identical relative folder structure but with different file name
	// - a file is moved to a different folder but with the original name
	// - a file is moved to a different folder with different name
	// 1. Import folder1, check staging
	// 2. Move all files from staging to colletion
	// 3. Import folder2, check staging
	// 4. Move all files from staging to collection
	// 5. Import folder1 again, check staging - must stay empty
	// 6. Import folder2 again, check staging - must stay empty
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
