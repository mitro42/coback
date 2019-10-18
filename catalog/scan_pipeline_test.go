package catalog

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	cth "github.com/mitro42/coback/catalogtesthelper"
	fsh "github.com/mitro42/coback/fshelper"
	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestWalkEmptyFolder(t *testing.T) {
	fs := afero.NewMemMapFs()
	fs.Mkdir("root", 0755)
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "root", &wg)
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

func TestWalkFolderOneLevel(t *testing.T) {
	fs := fsh.CreateSafeFs("test_data/subfolder")
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "", &wg)
	wg.Wait()

	expectedFiles := []string{"file1.bin", "file2.bin"}
	actualFiles := cth.ReadStringChannel(files)

	expectedSizes := []int64{1024, 1500}
	actualSizes := cth.ReadInt64Channel(sizes)
	th.Equals(t, expectedFiles, actualFiles)
	th.Equals(t, expectedSizes, actualSizes)
}

func TestWalkFolderRecursive(t *testing.T) {
	fs := fsh.CreateSafeFs("test_data")
	var wg sync.WaitGroup
	wg.Add(1)
	files, sizes := walkFolder(fs, "", &wg)
	wg.Wait()

	expectedFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	actualFiles := cth.ReadStringChannel(files)

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := cth.ReadInt64Channel(sizes)
	th.Equals(t, expectedFiles, actualFiles)
	th.Equals(t, expectedSizes, actualSizes)
}

func TestWalkFolderIgnoreCatalog(t *testing.T) {
	fs := fsh.CreateSafeFs("test_data")
	var wg sync.WaitGroup
	wg.Add(1)
	fs.Create(CatalogFileName)
	files, sizes := walkFolder(fs, "", &wg)
	wg.Wait()

	expectedFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	actualFiles := cth.ReadStringChannel(files)

	expectedSizes := []int64{1024, 1500, 1160, 1304}
	actualSizes := cth.ReadInt64Channel(sizes)
	th.Equals(t, expectedFiles, actualFiles)
	th.Equals(t, expectedSizes, actualSizes)
}

func TestFilterEmptyChannel(t *testing.T) {
	input := make(chan string)
	var wg sync.WaitGroup
	wg.Add(1)
	output := filterFiles(input, noFilter{}, &wg)
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
	var wg sync.WaitGroup
	wg.Add(1)
	output := filterFiles(input, noFilter{}, &wg)
	for _, item := range expected {
		input <- item
	}
	close(input)
	wg.Wait()
	actual := cth.ReadStringChannel(output)
	th.Equals(t, expected, actual)
}

func TestFilterExtension(t *testing.T) {
	inputFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	expected := []string{"test1.txt", "test2.txt"}
	input := make(chan string)
	var wg sync.WaitGroup
	wg.Add(1)
	output := filterFiles(input, ExtensionFilter("bin", "jpg"), &wg)
	for _, item := range inputFiles {
		input <- item
	}
	close(input)
	wg.Wait()
	actual := cth.ReadStringChannel(output)
	th.Equals(t, expected, actual)
}

func TestSumSizes(t *testing.T) {
	inputLength := 1000
	input := make(chan int64, inputLength+1)
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
	sumSizes(input, countBar, sizeBar, nil, &wg)
	wg.Wait()
	close(input)

	th.Equals(t, int64(inputLength), countBar.total)
	th.Equals(t, sum, sizeBar.total)
}

func TestCheckCatalogFileMissing(t *testing.T) {
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()
	fs := fsh.CreateSafeFs("test_data")
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
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
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
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
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
	fs := fsh.CreateSafeFs(path)
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
	fs := fsh.CreateSafeFs(path)
	c := ScanFolder(fs, "", noFilter{})
	inputFiles := make(chan string, 4)
	okFiles := make(chan string, 4)
	changedFiles := make(chan string, 4)
	var wg sync.WaitGroup
	wg.Add(1)

	checkExistingItems(fs, inputFiles, c, countBar, sizeBar, okFiles, changedFiles, &wg)
	inputFiles <- "test1.txt"
	inputFiles <- "subfolder/file1.bin"
	inputFiles <- "test2.txt"
	inputFiles <- ""
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
	fs := fsh.CreateSafeFs(path)
	c := ScanFolder(fs, "", noFilter{})
	inputFiles := make(chan string, 1)
	okFiles := make(chan string, 4)
	changedFiles := make(chan string, 4)
	var wg sync.WaitGroup
	wg.Add(1)

	go checkExistingItems(fs, inputFiles, c, countBar, sizeBar, okFiles, changedFiles, &wg)
	changeFileContent(fs, "test2.txt")
	inputFiles <- "subfolder/file1.bin"
	inputFiles <- "test2.txt"
	inputFiles <- "subfolder/file2.bin"
	inputFiles <- ""
	wg.Wait()
	expOk := map[string]bool{"subfolder/file2.bin": true, "subfolder/file1.bin": true}
	expChanged := map[string]bool{"test2.txt": true}
	th.Equals(t, expOk, collectFilesSync(okFiles))
	th.Equals(t, expChanged, collectFilesSync(changedFiles))

}

func TestCheckFilterByCatalogNoFiles(t *testing.T) {
	basePath, _ := os.Getwd()
	path := filepath.Join(basePath, "test_data")
	fs := fsh.CreateSafeFs(path)
	c := ScanFolder(fs, "", noFilter{})
	input := make(chan string, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	known, unknown := filterByCatalog(input, c, &wg)
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
	var wg sync.WaitGroup
	wg.Add(1)
	known, unknown := filterByCatalog(input, c, &wg)
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
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	filter := ExtensionFilter("txt")
	c := ScanFolder(fs, "", filter)

	inputFiles := []string{"subfolder/file1.bin", "subfolder/file2.bin", "test1.txt", "test2.txt"}
	input := make(chan string, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	known, unknown := filterByCatalog(input, c, &wg)
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

func TestReadCatalogItems(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()

	inputFiles := []string{"subfolder/file1.bin", "test1.txt", "subfolder/file2.bin", "test2.txt"}
	input := make(chan string, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	catalogItems := readCatalogItems(fs, input, countBar, sizeBar, &wg)
	for _, item := range inputFiles {
		input <- item
	}
	input <- ""

	outputPaths := make([]string, 0, 0)
	for range inputFiles {
		item := <-catalogItems
		expectedItem, err := NewItem(fs, item.Path)
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
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	countBar := newMockProgressBar()
	sizeBar := newMockProgressBar()

	input := make(chan string, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	catalogItems := readCatalogItems(fs, input, countBar, sizeBar, &wg)
	input <- ""

	wg.Wait()
	th.Equals(t, Item{}, <-catalogItems)
	th.Equals(t, 0, len(catalogItems))
	th.Equals(t, 0, countBar.incrByCount)
	th.Equals(t, 0, sizeBar.incrByCount)
}

func TestSaveCatalogEmpty(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	items := make(chan Item)
	result := make(chan Catalog, 1)
	var wg sync.WaitGroup
	wg.Add(1)

	go saveCatalog(fs, CatalogFileName, items, result, &wg)
	items <- Item{}
	wg.Wait()
	c := <-result
	th.Equals(t, NewCatalog(), c)
}

func TestSaveCatalog(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	items := make(chan Item)
	result := make(chan Catalog, 1)
	var wg sync.WaitGroup
	wg.Add(1)

	go saveCatalog(fs, CatalogFileName, items, result, &wg)
	item1, err := NewItem(fs, "test1.txt")
	th.Ok(t, err)
	item2, err := NewItem(fs, "subfolder/file1.bin")
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
