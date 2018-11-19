package catalog

import (
	"sync"
	"testing"

	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestWalkEmptyFolder(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("root", 0755)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "root", done, &wg)
	wg.Wait()
	fileFound := false
	select {
	case <-files:
		fileFound = true
	default:
	}

	sizeFound := false
	select {
	case <-sizes:
		sizeFound = true
	default:
	}
	th.Equals(t, false, fileFound)
	th.Equals(t, false, sizeFound)
}

func TestWalkFolderOneLevel(t *testing.T) {
	fs := createSafeFs("test_data/subfolder")
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "", done, &wg)
	wg.Wait()

	expectedFiles := []string{"file1.bin", "file2.bin"}
	actualFiles := make([]string, 0, 2)
	for file := range files {
		if file == "" {
			break
		}
		actualFiles = append(actualFiles, file)
	}

	expectedSizes := []int64{1024, 1500}
	actualSizes := make([]int64, 0, 2)
	for size := range sizes {
		if size == -1 {
			break
		}
		actualSizes = append(actualSizes, size)
	}
	th.Equals(t, expectedFiles, actualFiles)
	th.Equals(t, expectedSizes, actualSizes)
}

func TestWalkFolderRecursive(t *testing.T) {
	fs := createSafeFs("test_data")
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "", done, &wg)
	wg.Wait()

	expectedFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	actualFiles := make([]string, 0, 2)
	for file := range files {
		if file == "" {
			break
		}
		actualFiles = append(actualFiles, file)
	}

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := make([]int64, 0, 2)
	for size := range sizes {
		if size == -1 {
			break
		}
		actualSizes = append(actualSizes, size)
	}
	th.Equals(t, expectedFiles, actualFiles)
	th.Equals(t, expectedSizes, actualSizes)
}
