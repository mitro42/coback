package catalog

import (
	"sync"
	"testing"
	"time"

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
	case file := <-files:
		fileFound = file != ""
	default:
	}

	sizeFound := false
	select {
	case size := <-sizes:
		sizeFound = size != -1
	default:
	}
	th.Equals(t, false, fileFound)
	th.Equals(t, false, sizeFound)
}

func readFilesChannel(files <-chan string) []string {
	ret := make([]string, 0)
	closingElementFound := false
	for file := range files {
		if file == "" {
			closingElementFound = true
			break
		}
		ret = append(ret, file)
	}
	if !closingElementFound {
		panic("closing element not found")
	}
	return ret
}

func readSizesChannel(sizes <-chan int64) []int64 {
	ret := make([]int64, 0)
	closingElementFound := false
	for size := range sizes {
		if size == -1 {
			closingElementFound = true
			break
		}
		ret = append(ret, size)
	}
	if !closingElementFound {
		panic("closing element not found")
	}
	return ret
}

func isPrefixStringSlice(full []string, prefix []string) bool {
	if len(prefix) > len(full) {
		return false
	}

	for idx, val := range prefix {
		if val != full[idx] {
			return false
		}
	}
	return true
}

func isPrefixInt64Slice(full []int64, prefix []int64) bool {
	if len(prefix) > len(full) {
		return false
	}

	for idx, val := range prefix {
		if val != full[idx] {
			return false
		}
	}
	return true
}

func TestWalkFolderOneLevel(t *testing.T) {
	fs := createSafeFs("test_data/subfolder")
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "", done, &wg)
	wg.Wait()

	expectedFiles := []string{"file1.bin", "file2.bin"}
	actualFiles := readFilesChannel(files)

	expectedSizes := []int64{1024, 1500}
	actualSizes := readSizesChannel(sizes)
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
	actualFiles := readFilesChannel(files)

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := readSizesChannel(sizes)
	th.Equals(t, expectedFiles, actualFiles)
	th.Equals(t, expectedSizes, actualSizes)
}

func TestWalkFolderRecursiveInterrupt(t *testing.T) {
	fs := createSafeFs("test_data")
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "", done, &wg)
	time.Sleep(40 * time.Microsecond)
	close(done)
	wg.Wait()

	expectedFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	actualFiles := readFilesChannel(files)

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := readSizesChannel(sizes)
	th.Equals(t, true, isPrefixStringSlice(expectedFiles, actualFiles))
	th.Equals(t, true, isPrefixInt64Slice(expectedSizes, actualSizes))
}
