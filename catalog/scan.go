package catalog

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

// ProgressBar is the minimal progress bar interface used in CoBack.
// It enables easy mocking of the progress bars in unit tests
type ProgressBar interface {
	SetTotal(total int64, final bool)
	IncrBy(n int, wdd ...time.Duration)
	Current() int64
}

// CheckResult contains the details of catalog checked against a folder in the file system.
// Ok (set of paths): these files have the same size, modification time and content in the catalog and the FS
// Add (set of paths): these files are present in the FS but not int the catalog, have to be added
// Delete (set of paths): these files are present in the catalog but not present in the FS anymore, have to be deleted
// Update (set of paths): these files have different size, modification time or content in the catalog and the FS.
//                          Probably a full re-scan should be done
type CheckResult struct {
	Ok     map[string]bool
	Add    map[string]bool
	Delete map[Checksum]bool
	Update map[string]bool
}

// NewCheckResult creates a new CheckResult struct
func NewCheckResult() CheckResult {
	return CheckResult{
		Ok:     make(map[string]bool),
		Add:    make(map[string]bool),
		Delete: make(map[Checksum]bool),
		Update: make(map[string]bool),
	}
}

func walkFolder(fs afero.Fs, root string, wg *sync.WaitGroup) (<-chan string, <-chan int64) {
	files := make(chan string, 100000)
	sizes := make(chan int64, 100000)
	if exist, err := afero.DirExists(fs, root); err != nil || !exist {
		log.Fatalf("The folder '%v' doesn't exist", root)
	}
	go func() {
		defer wg.Done()
		afero.Walk(fs, root, func(path string, fi os.FileInfo, err error) error {
				if !fi.IsDir() && fi.Name() != CatalogFileName {
					files <- path
					sizes <- fi.Size()
				}
				return nil
		})
		files <- ""
		sizes <- -1
	}()
	return files, sizes
}

func filterFiles(files <-chan string, filter FileFilter, wg *sync.WaitGroup) chan string {
	filtered := make(chan string, 10000)

	go func() {
		defer wg.Done()
		for file := range files {
				if file == "" {
				break
				} else if filter.Include(file) {
					filtered <- file
				}
			}
		filtered <- ""
	}()
	return filtered
}

func catalogFile(fs afero.Fs, path string, out chan Item, countBar ProgressBar, sizeBar ProgressBar) {
	start := time.Now()
	item, err := newItem(fs, path)
	if err != nil {
		log.Printf("Cannot read file '%v'", path)
	} else {
		sizeBar.IncrBy(int(item.Size), time.Since(start))
		countBar.IncrBy(1, time.Since(start))
		out <- *item
	}
}

// checkCatalogFile checks a given file (metadata and content) against a catalog
// Returns true if the file at path exist and the content matches the catalog.
func checkCatalogFile(fs afero.Fs, path string, c Catalog, countBar ProgressBar, sizeBar ProgressBar, ok chan<- string, changed chan<- string) error {
	start := time.Now()
	item, err := newItem(fs, path)
	if err != nil {
		return errors.Errorf("Cannot read file '%v'", path)
	}

	sizeBar.IncrBy(int(item.Size), time.Since(start))
	countBar.IncrBy(1, time.Since(start))
	itemInCatalog, err := c.Item(path)

	if err != nil {
		return errors.Errorf("Cannot find file in catalog '%v'", path)
	}

	if *item == itemInCatalog {
		ok <- path
	} else {
		changed <- path
	}

	return nil
}

// readCatalogItems creates the CatalogItems for the incoming paths
// Can be interrupted with a message sent to the done channel,
// The processing can be interrupted by a message sent to the done channel.
// The paths channel must be buffered.
func readCatalogItems(fs afero.Fs,
	paths chan string,
	countBar ProgressBar,
	sizeBar ProgressBar,
	globalWg *sync.WaitGroup) <-chan Item {

	out := make(chan Item, 10)
	var wg sync.WaitGroup
	const concurrency = 6
	wg.Add(concurrency)
	go func() {
		defer globalWg.Done()
		for i := 0; i < concurrency; i++ {
			go func() {
				for path := range paths {
						if path == "" {
						paths <- "" // make one of the siblings stop
							break
						}
						catalogFile(fs, path, out, countBar, sizeBar)
					}
				wg.Done()
			}()
		}
		wg.Wait()
		out <- Item{}
		sizeBar.SetTotal(sizeBar.Current(), true)
		countBar.SetTotal(countBar.Current(), true)
	}()

	return out
}

// checkExistingItems checks the incoming files against a catalog
// Processes the files in the paths channel, and calls checkCatalogFile on each of them.
// At the first error sends a message on failed channel but carry on may processing the input until interrupted.
// At each steps updated the progress bars with the number and size of processed files.
// Can be interrupted at any time by sending a message to the done channel.
// The paths channel must be buffered.
func checkExistingItems(fs afero.Fs,
	paths chan string,
	c Catalog,
	countBar ProgressBar,
	sizeBar ProgressBar,
	ok chan<- string,
	changed chan<- string,
	globalWg *sync.WaitGroup) {

	var wg sync.WaitGroup
	const concurrency = 6
	wg.Add(concurrency)
	go func() {
		defer globalWg.Done()
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				for path := range paths {
						if path == "" {
						paths <- "" // make one of the siblings stop
							break
						}
						if err := checkCatalogFile(fs, path, c, countBar, sizeBar, ok, changed); err != nil {
							log.Println(err)
						}
					}
			}()
		}
		wg.Wait()
		ok <- ""
		changed <- ""
		sizeBar.SetTotal(sizeBar.Current(), true)
		countBar.SetTotal(countBar.Current(), true)
	}()

}

// sumSizes calculates the sum of the numbers read from the sizes channel.
// It can be interrupted with the done channel
func sumSizes(sizes <-chan int64, countBar ProgressBar, sizeBar ProgressBar, fileCount chan<- int64, wg *sync.WaitGroup) {
	defer wg.Done()
	total := int64(0)
	count := int64(0)
	for s := range sizes {
			if s == -1 {
				break
			}
			total += s
			count++
			sizeBar.SetTotal(total, false)
			countBar.SetTotal(count, false)
		}
	if fileCount != nil {
		fileCount <- count
}
}

func createProgressBars() (*mpb.Progress, ProgressBar, ProgressBar) {
	p := mpb.New(
		mpb.WithRefreshRate(100 * time.Millisecond),
	)
	countName := "Number of Files"
	countBar := p.AddBar(int64(0),
		mpb.PrependDecorators(
			decor.Name(countName, decor.WC{W: len(countName) + 2, C: decor.DidentRight}),
			decor.CountersNoUnit("%8d / %8d "),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)
	sizeName := "Processed Size"
	sizeBar := p.AddBar(int64(0),
		mpb.PrependDecorators(
			decor.Name(sizeName, decor.WC{W: len(countName) + 2, C: decor.DidentRight}),
			decor.CountersKibiByte("%8.1f / %8.1f "),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.AverageSpeed(decor.UnitKiB, " %6.1f"),
		),
	)
	return p, countBar, sizeBar
}

func saveCatalog(fs afero.Fs, catalogPath string, items <-chan Item,
	result chan<- Catalog, wg *sync.WaitGroup) {
	defer wg.Done()
	lastSave := time.Now()
	c := NewCatalog()
	for item := range items {
			if (item == Item{}) {
			break
			}
			err := c.Add(item)
			if err != nil {
				log.Printf("Cannot save catalog: %v", err)
			}
			if time.Since(lastSave).Seconds() > 5.0 {
				lastSave = time.Now()
				err := c.Write(fs, catalogPath)
				if err != nil {
					log.Printf("Failed to update catalog: %v", err)
				}
			}
		}

	err := c.Write(fs, catalogPath)
	if err != nil {
		log.Printf("Failed to update catalog: %v", err)
	}
	result <- c
}

// ScanFolder recursively scans the root folder and adds all files to the catalog
func ScanFolder(fs afero.Fs, root string, filter FileFilter) Catalog {
	var wg sync.WaitGroup
	p, countBar, sizeBar := createProgressBars()
	wg.Add(5)
	files, sizes := walkFolder(fs, root, &wg)
	filteredFiles := filterFiles(files, filter, &wg)
	items := readCatalogItems(fs, filteredFiles, countBar, sizeBar, &wg)
	go sumSizes(sizes, countBar, sizeBar, nil, &wg)
	result := make(chan Catalog)
	catalogFilePath := filepath.Join(root, CatalogFileName)
	go saveCatalog(fs, catalogFilePath, items, result, &wg)
	ret := <-result
	p.Wait()
	return ret
}

// Scan recursively scans the whole file system
func Scan(fs afero.Fs) Catalog {
	return ScanFolder(fs, ".", noFilter{})
}

// filterByCatalog separate the incoming files (typically contents of the file system)
// to two channels based on whether they are present in the catalog or not.
// If an file read from the files channel is in the catalog (only the path is checked, no metadata, no contents)
// it is put to known otherwise to unknown.
// The processing can be interrupted by a message sent to the done channel.
func filterByCatalog(files <-chan string, c Catalog, wg *sync.WaitGroup) (known chan string, unknown chan string) {
	known = make(chan string, 100)
	unknown = make(chan string, 100)
	go func() {
		defer wg.Done()
		for file := range files {
				if file == "" {
					known <- ""
					unknown <- ""
				break
				}
				if _, err := c.Item(file); err == nil {
					known <- file
				} else {
					unknown <- file
			}
		}
	}()
	return
}

func collectFiles(c <-chan string, m map[string]bool, wg *sync.WaitGroup) {
	for file := range c {
		if file == "" {
			break
		}
		m[file] = true
	}
	wg.Done()
}

// Check scans a folder and compare its contents to the contents of the catalog.
// Compares all data and content. It performs a full scan but stops at the first mismatch.
// Returns true if the catalog is consistent with the file system,
// and false if there is a mismatch
func Check(fs afero.Fs, c Catalog, filter FileFilter) CheckResult {
	okFiles := make(chan string, 1)
	changedFiles := make(chan string, 1)
	var wg sync.WaitGroup
	wg.Add(8)

	p, countBar, sizeBar := createProgressBars()

	files, sizes := walkFolder(fs, ".", &wg)
	filteredFiles := filterFiles(files, filter, &wg)
	knownFiles, unknownFiles := filterByCatalog(filteredFiles, c, &wg)
	checkExistingItems(fs, knownFiles, c, countBar, sizeBar, okFiles, changedFiles, &wg)
	go sumSizes(sizes, countBar, sizeBar, nil, &wg)
	ret := NewCheckResult()

	collectFiles(okFiles, ret.Ok, &wg)
	collectFiles(changedFiles, ret.Update, &wg)
	collectFiles(unknownFiles, ret.Add, &wg)
			wg.Wait()

	p.Wait()
	log.Printf("Check done, ok: %v, to update: %v, to add: %v", len(ret.Ok), len(ret.Update), len(ret.Add))
	return ret
}
