package catalog

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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

func readStringChannel(files <-chan string) []string {
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

func readInt64Channel(sizes <-chan int64) []int64 {
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
	actualFiles := readStringChannel(files)

	expectedSizes := []int64{1024, 1500}
	actualSizes := readInt64Channel(sizes)
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
	actualFiles := readStringChannel(files)

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := readInt64Channel(sizes)
	th.Equals(t, expectedFiles, actualFiles)
	th.Equals(t, expectedSizes, actualSizes)
}

func TestWalkFolderIgnoreCatalog(t *testing.T) {
	fs := createSafeFs("test_data")
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	fs.Create(CatalogFileName)
	files, sizes := walkFolder(fs, "", done, &wg)
	wg.Wait()

	expectedFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	actualFiles := readStringChannel(files)

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := readInt64Channel(sizes)
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
	actualFiles := readStringChannel(files)

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := readInt64Channel(sizes)
	th.Equals(t, true, isPrefixStringSlice(expectedFiles, actualFiles))
	th.Equals(t, true, isPrefixInt64Slice(expectedSizes, actualSizes))
}

func TestFilterEmptyChannel(t *testing.T) {
	input := make(chan string)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	output := filterFiles(input, noFilter{}, done, &wg)
	close(input)
	wg.Wait()
	itemFound := false
	select {
	case item := <-output:
		itemFound = item != ""
	default:
	}
	th.Equals(t, false, itemFound)
}

func TestFilterNoFilter(t *testing.T) {
	expected := []string{"orange", "pear", "apple", "melon"}
	input := make(chan string)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	output := filterFiles(input, noFilter{}, done, &wg)
	for _, item := range expected {
		input <- item
	}
	close(input)
	wg.Wait()
	actual := readStringChannel(output)
	th.Equals(t, expected, actual)
}

func TestFilterExtension(t *testing.T) {
	inputFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	expected := []string{"test1.txt", "test2.txt"}
	input := make(chan string)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	output := filterFiles(input, ExtensionFilter("bin", "jpg"), done, &wg)
	for _, item := range inputFiles {
		input <- item
	}
	close(input)
	wg.Wait()
	actual := readStringChannel(output)
	th.Equals(t, expected, actual)
}

func TestFilterExtensionInterrupt(t *testing.T) {
	inputFiles := []string{"subfolder/file1.bin", "test1.txt", "test2.txt", "subfolder/file2.bin"}
	expected := []string{"test1.txt"}
	input := make(chan string)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	output := filterFiles(input, ExtensionFilter("bin", "jpg", "xt", "subfolder"), done, &wg)
	input <- inputFiles[0]
	input <- inputFiles[1]
	close(done)
	close(input)
	wg.Wait()
	actual := readStringChannel(output)
	th.Equals(t, expected, actual)
}

func TestSumSizes(t *testing.T) {
	inputLength := 1000
	input := make(chan int64, inputLength+1)
	done := make(chan struct{})
	sum := int64(0)
	for i := 0; i < inputLength; i++ {
		v := int64(rand.Uint32())
		sum += v
		input <- v
	}
	input <- -1

	var wg sync.WaitGroup
	wg.Add(1)
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	sumSizes(input, countBar, sizeBar, nil, done, &wg)
	wg.Wait()
	close(input)

	th.Equals(t, int64(inputLength), countBar.total)
	th.Equals(t, sum, sizeBar.total)
}

func TestSumSizesInterrupt(t *testing.T) {
	inputLength := 100
	input := make(chan int64, inputLength+1)
	done := make(chan struct{})
	sizes := make([]int64, 0, inputLength)

	go func() {
		for i := 0; i < inputLength; i++ {
			time.Sleep(10 * time.Microsecond)
			v := int64(rand.Uint32())
			sizes = append(sizes, v)
			input <- v
		}
		input <- -1
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	go sumSizes(input, countBar, sizeBar, nil, done, &wg)
	time.Sleep(1000 * time.Microsecond)
	close(done)
	wg.Wait()

	sum := int64(0)
	match := sizeBar.total == 0 && countBar.total == 0
	for i := 0; i < len(sizes); i++ {
		sum += sizes[i]
		match = match || (sizeBar.total == sum && countBar.total == int64(i+1))
	}

	th.Assert(t, match, "Count or size mismatch")
}

func TestCheckCatalogFileMissing(t *testing.T) {
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	fs := createSafeFs("test_data")
	c := NewCatalog()
	okFiles := make(chan string)
	changedFiles := make(chan string)
	th.Nok(t, checkCatalogFile(fs, "no_such_file", c, countBar, sizeBar, okFiles, changedFiles), "Cannot read file 'no_such_file'")
	th.Equals(t, 0, countBar.incrByCount)
	th.Equals(t, 0, sizeBar.incrByCount)
	th.Equals(t, 0, len(okFiles))
	th.Equals(t, 0, len(changedFiles))
}

func TestCheckCatalogFileNotInCatalog(t *testing.T) {
	basePath, _ := os.Getwd()
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("txt")
	c := ScanFolder(fs, "", filter)
	okFiles := make(chan string, 1)
	changedFiles := make(chan string, 1)
	th.Nok(t, checkCatalogFile(fs, "test1.txt", c, countBar, sizeBar, okFiles, changedFiles), "Cannot find file in catalog 'test1.txt'")
	th.Equals(t, 1, countBar.incrByCount)
	th.Equals(t, int64(1), countBar.value)
	th.Equals(t, 1, sizeBar.incrByCount)
	th.Equals(t, int64(1160), sizeBar.value)
	th.Equals(t, 0, len(okFiles))
	th.Equals(t, 0, len(changedFiles))
}

func TestCheckCatalogFileSuccess(t *testing.T) {
	basePath, _ := os.Getwd()
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	c := ScanFolder(fs, "", noFilter{})
	okFiles := make(chan string, 1)
	changedFiles := make(chan string, 1)
	th.Ok(t, checkCatalogFile(fs, "test1.txt", c, countBar, sizeBar, okFiles, changedFiles))
	th.Equals(t, 1, countBar.incrByCount)
	th.Equals(t, int64(1), countBar.value)
	th.Equals(t, 1, sizeBar.incrByCount)
	th.Equals(t, int64(1160), sizeBar.value)
	th.Equals(t, "test1.txt", <-okFiles)
	th.Equals(t, 0, len(changedFiles))
}

func TestCheckCatalogFileMismatch(t *testing.T) {
	basePath, _ := os.Getwd()
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	path := filepath.Join(basePath, "test_data")
	fs := createSafeFs(path)
	c := ScanFolder(fs, "", noFilter{})
	modifiedFile := "test1.txt"
	changeFileContent(fs, modifiedFile)
	okFiles := make(chan string, 1)
	changedFiles := make(chan string, 1)
	th.Ok(t, checkCatalogFile(fs, modifiedFile, c, countBar, sizeBar, okFiles, changedFiles))
	th.Equals(t, 1, countBar.incrByCount)
	th.Equals(t, int64(1), countBar.value)
	th.Equals(t, 1, sizeBar.incrByCount)
	th.Equals(t, int64(1175), sizeBar.value)
	th.Equals(t, 0, len(okFiles))
	th.Equals(t, "test1.txt", <-changedFiles)
}

func collectFilesSync(c <-chan string) map[string]bool {
	ret := make(map[string]bool)
	for file := range c {
		if file == "" {
			break
		}
		ret[file] = true
	}
	return ret
}

func TestCheckExistingItemsSuccess(t *testing.T) {
	basePath, _ := os.Getwd()
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	path := filepath.Join(basePath, "test_data")
	fs := createSafeFs(path)
	c := ScanFolder(fs, "", noFilter{})
	inputFiles := make(chan string, 4)
	okFiles := make(chan string, 4)
	changedFiles := make(chan string, 4)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go checkExistingItems(fs, inputFiles, c, countBar, sizeBar, okFiles, changedFiles, done, &wg)
	inputFiles <- "test1.txt"
	inputFiles <- "subfolder/file1.bin"
	inputFiles <- "test2.txt"
	inputFiles <- ""
	close(done)
	wg.Wait()
	th.Equals(t, 3, countBar.incrByCount)
	th.Equals(t, int64(3), countBar.value)
	th.Equals(t, 3, sizeBar.incrByCount)
	th.Equals(t, int64(3488), sizeBar.value)
	expOk := map[string]bool{"test1.txt": true, "subfolder/file1.bin": true, "test2.txt": true}
	th.Equals(t, expOk, collectFilesSync(okFiles))
}

func TestCheckExistingItemsMismatch(t *testing.T) {
	basePath, _ := os.Getwd()
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	path := filepath.Join(basePath, "test_data")
	fs := createSafeFs(path)
	c := ScanFolder(fs, "", noFilter{})
	inputFiles := make(chan string, 1)
	okFiles := make(chan string, 4)
	changedFiles := make(chan string, 4)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go checkExistingItems(fs, inputFiles, c, countBar, sizeBar, okFiles, changedFiles, done, &wg)
	changeFileContent(fs, "test2.txt")
	inputFiles <- "subfolder/file1.bin"
	inputFiles <- "test2.txt"
	inputFiles <- "subfolder/file2.bin"
	close(done)
	wg.Wait()
	expOk := map[string]bool{"subfolder/file2.bin": true, "subfolder/file1.bin": true}
	expChanged := map[string]bool{"test2.txt": true}
	th.Equals(t, expOk, collectFilesSync(okFiles))
	th.Equals(t, expChanged, collectFilesSync(changedFiles))

}

func TestCheckFilterByCatalogNoFiles(t *testing.T) {
	basePath, _ := os.Getwd()
	path := filepath.Join(basePath, "test_data")
	fs := createSafeFs(path)
	c := ScanFolder(fs, "", noFilter{})
	input := make(chan string, 1)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	known, unknown := filterByCatalog(input, c, done, &wg)
	input <- ""
	wg.Wait()
	th.Equals(t, "", <-known)
	th.Equals(t, "", <-unknown)
	th.Equals(t, 0, len(known))
	th.Equals(t, 0, len(unknown))
}

func TestCheckFilterByCatalogEmptyCatalog(t *testing.T) {
	inputFiles := map[string]bool{"subfolder/file1.bin": true, "subfolder/file2.bin": true, "test1.txt": true, "test2.txt": true}
	c := NewCatalog()
	input := make(chan string, 10)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	known, unknown := filterByCatalog(input, c, done, &wg)
	for k := range inputFiles {
		input <- k
	}
	input <- ""

	wg.Wait()
	th.Equals(t, "", <-known)
	th.Equals(t, inputFiles, collectFilesSync(unknown))
}

func TestCheckFilterByCatalogMixed(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("txt")
	c := ScanFolder(fs, "", filter)

	inputFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	input := make(chan string, 10)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	known, unknown := filterByCatalog(input, c, done, &wg)
	for _, item := range inputFiles {
		input <- item
	}
	input <- ""
	expKnown := map[string]bool{"subfolder/file1.bin": true, "subfolder/file2.bin": true}
	expUnknown := map[string]bool{"test1.txt": true, "test2.txt": true}

	th.Equals(t, expKnown, collectFilesSync(known))
	th.Equals(t, expUnknown, collectFilesSync(unknown))
	wg.Wait()
}

func TestCheckFilterByCatalogInterrupted(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("txt")
	c := ScanFolder(fs, "", filter)

	inputFiles := []string{"subfolder/file1.bin", "test1.txt", "subfolder/file2.bin", "test2.txt"}
	input := make(chan string, 10)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	known, unknown := filterByCatalog(input, c, done, &wg)
	for idx, item := range inputFiles {
		if idx == 2 {
			time.Sleep(time.Millisecond * 50)
			close(done)
		}
		input <- item
	}
	input <- ""
	go func() {
		th.Equals(t, inputFiles[0], <-known)
		th.Equals(t, inputFiles[1], <-unknown)
	}()

	wg.Wait()
}

func TestReadCatalogItems(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()

	inputFiles := []string{"subfolder/file1.bin", "test1.txt", "subfolder/file2.bin", "test2.txt"}
	input := make(chan string, 10)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	catalogItems := readCatalogItems(fs, input, countBar, sizeBar, done, &wg)
	for _, item := range inputFiles {
		input <- item
	}
	input <- ""

	outputPaths := make([]string, 0, 0)
	for range inputFiles {
		item := <-catalogItems
		expectedItem, err := newItem(fs, item.Path)
		th.Ok(t, err)
		th.Equals(t, *expectedItem, item)
		outputPaths = append(outputPaths, item.Path)
	}

	wg.Wait()
	for _, outputPath := range outputPaths {
		found := false
		for _, inputPath := range inputFiles {
			if inputPath == outputPath {
				found = true
				break
			}
		}
		th.Assert(t, found, fmt.Sprintf("path '%v' returned buy readCatalogItems was not in the input", outputPath))
	}
	th.Equals(t, 4, countBar.incrByCount)
	th.Equals(t, int64(4), countBar.value)
	th.Equals(t, 4, sizeBar.incrByCount)
	th.Equals(t, int64(4988), sizeBar.value)
}

func TestReadCatalogItemsEmpty(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()

	input := make(chan string, 10)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	catalogItems := readCatalogItems(fs, input, countBar, sizeBar, done, &wg)
	input <- ""

	wg.Wait()
	th.Equals(t, Item{}, <-catalogItems)
	th.Equals(t, 0, len(catalogItems))
	th.Equals(t, 0, countBar.incrByCount)
	th.Equals(t, 0, sizeBar.incrByCount)
}

func TestReadCatalogItemsInterrupt(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()

	inputFiles := []string{"subfolder/file1.bin", "test1.txt", "subfolder/file2.bin", "test2.txt"}
	input := make(chan string, 10)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	catalogItems := readCatalogItems(fs, input, countBar, sizeBar, done, &wg)
	for idx, item := range inputFiles {
		if idx == 2 {
			close(done)
			time.Sleep(time.Millisecond * 50)
		}
		input <- item
	}

	wg.Wait()
	outputPaths := make([]string, 0, 0)
	for item := range catalogItems {
		if (item == Item{}) {
			break
		}
		expectedItem, err := newItem(fs, item.Path)
		th.Ok(t, err)
		th.Equals(t, *expectedItem, item)
		outputPaths = append(outputPaths, item.Path)
	}

	for _, outputPath := range outputPaths {
		found := false
		for _, inputPath := range inputFiles {
			if inputPath == outputPath {
				found = true
				break
			}
		}
		th.Assert(t, found, fmt.Sprintf("path '%v' returned buy readCatalogItems was not in the input", outputPath))
	}
	th.Equals(t, 2, countBar.incrByCount)
	th.Equals(t, 2, sizeBar.incrByCount)
	th.Equals(t, int64(2184), sizeBar.value)
}

func TestSaveCatalogEmpty(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	items := make(chan Item)
	result := make(chan Catalog, 1)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go saveCatalog(fs, CatalogFileName, items, result, done, &wg)
	items <- Item{}
	wg.Wait()
	c := <-result
	th.Equals(t, NewCatalog(), c)
}

func TestSaveCatalog(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	items := make(chan Item)
	result := make(chan Catalog, 1)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go saveCatalog(fs, CatalogFileName, items, result, done, &wg)
	item1, err := newItem(fs, "test1.txt")
	th.Ok(t, err)
	item2, err := newItem(fs, "subfolder/file1.bin")
	th.Ok(t, err)
	items <- *item1
	items <- *item2
	items <- Item{}
	wg.Wait()
	c := <-result
	expectedCatalog := NewCatalog()
	expectedCatalog.Add(*item1)
	expectedCatalog.Add(*item2)
	th.Equals(t, expectedCatalog, c)
}

func TestSaveCatalogInterrupt(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	items := make(chan Item, 10)
	result := make(chan Catalog, 1)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go saveCatalog(fs, CatalogFileName, items, result, done, &wg)
	item1, err := newItem(fs, "test1.txt")
	th.Ok(t, err)
	item2, err := newItem(fs, "subfolder/file1.bin")
	th.Ok(t, err)

	items <- *item1
	time.Sleep(40 * time.Microsecond)
	done <- struct{}{}
	time.Sleep(40 * time.Microsecond)
	items <- *item2
	time.Sleep(40 * time.Microsecond)
	items <- Item{}
	wg.Wait()
	c := <-result
	expectedCatalog := NewCatalog()
	expectedCatalog.Add(*item1)
	th.Equals(t, expectedCatalog, c)
}
