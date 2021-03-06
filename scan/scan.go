package scan

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mitro42/coback/catalog"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// FileSystemDiff contains the details of catalog checked against a folder in the file system.
// Ok (set of paths): these files have the same size, modification time and content in the catalog and the FS
// Add (set of paths): these files are present in the FS but not int the catalog, have to be added
// Delete (set of paths): these files are present in the catalog but not present in the FS anymore, have to be deleted
// Update (set of paths): these files have different size, modification time or content in the catalog and the FS.
//                          Probably a full re-scan should be done
type FileSystemDiff struct {
	Ok     map[string]bool
	Add    map[string]bool
	Delete map[string]bool
	Update map[string]bool
}

// NewFileSystemDiff creates a new FileSystemDiff struct
func NewFileSystemDiff() FileSystemDiff {
	return FileSystemDiff{
		Ok:     make(map[string]bool),
		Add:    make(map[string]bool),
		Delete: make(map[string]bool),
		Update: make(map[string]bool),
	}
}

// Asynchronously enumerates all files in a folder, returns a channel that will
// contain all the relative paths.
// When the enumeration is finished an empty string is sent to the channel as the last item.
func walkFolder(fs afero.Fs, root string, wg *sync.WaitGroup) <-chan string {
	files := make(chan string, 100000)
	if exist, err := afero.DirExists(fs, root); err != nil || !exist {
		log.Fatalf("The folder '%v' doesn't exist", root)
	}
	go func() {
		defer wg.Done()
		afero.Walk(fs, root, func(path string, fi os.FileInfo, err error) error {
			if !fi.IsDir() && fi.Name() != catalog.CatalogFileName {
				files <- path
			}
			return nil
		})
		files <- ""
	}()
	return files
}

// filters the paths read from the files channel and the one that pass the filter will be sent to the returned channel
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

func catalogFile(fs afero.Fs, path string, out chan catalog.Item, pb DoubleProgressBar) {
	item, err := catalog.NewItem(fs, path)
	if err != nil {
		log.Printf("Cannot read file '%v'", path)
	} else {
		pb.IncrBy(int(item.Size))
		out <- *item
	}
}

// checkCatalogFile checks a given file (metadata and content) against a catalog
// The file's path is sent to the ok if everything matches the catalog and to the changed channel otherwise.
// Returns error if cannot read the file or it's not in the catalog.
func checkCatalogFile(fs afero.Fs, path string, c catalog.Catalog, pb DoubleProgressBar, ok chan<- string, changed chan<- string) error {
	item, err := catalog.NewItem(fs, path)
	if err != nil {
		return errors.Errorf("Cannot read file '%v'", path)
	}

	pb.IncrBy(int(item.Size))
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

// quickCheckCatalogFile checks a given file against a catalog.
// It only checks the file size and modification time.
// If both are unchanged, it is assumed that the contents are the same too and the path is sent to the ok channel.
// If either the size or the modification time is different, the path is sent to the changed channel.
// Returns error if cannot read the file or it's not in the catalog.
func quickCheckCatalogFile(fs afero.Fs, path string, c catalog.Catalog, pb DoubleProgressBar, ok chan<- string, changed chan<- string) error {
	fi, err := fs.Stat(path)
	if err != nil {
		return errors.Wrap(err, "Cannot get file info")
	}

	size := fi.Size()
	modificationTime := fi.ModTime().Format(time.RFC3339Nano)

	pb.IncrBy(int(size))

	itemInCatalog, err := c.Item(path)
	if err != nil {
		return errors.Errorf("Cannot find file in catalog '%v'", path)
	}

	if size == itemInCatalog.Size && modificationTime == itemInCatalog.ModificationTime {
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
	pb DoubleProgressBar,
	globalWg *sync.WaitGroup) <-chan catalog.Item {

	out := make(chan catalog.Item, 10)
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
					catalogFile(fs, path, out, pb)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		out <- catalog.Item{}
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
	deepCheck bool,
	paths chan string,
	c catalog.Catalog,
	pb DoubleProgressBar,
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
					if deepCheck {
						if err := checkCatalogFile(fs, path, c, pb, ok, changed); err != nil {
							log.Println(err)
						}
					} else {
						if err := quickCheckCatalogFile(fs, path, c, pb, ok, changed); err != nil {
							log.Println(err)
						}

					}
				}
			}()
		}
		wg.Wait()
		ok <- ""
		changed <- ""
	}()

}

func updateAndSaveCatalog(fs afero.Fs, c catalog.Catalog, catalogPath string, items <-chan catalog.Item,
	result chan<- catalog.Catalog, wg *sync.WaitGroup) {
	defer wg.Done()
	ret := c.Clone()
	lastSave := time.Now()
	for item := range items {
		if (item == catalog.Item{}) {
			break
		}
		err := ret.Add(item)
		if err != nil {
			log.Printf("Cannot save catalog: %v", err)
		}
		if time.Since(lastSave).Seconds() > 5.0 {
			lastSave = time.Now()
			err := ret.Write(fs)
			if err != nil {
				log.Printf("Failed to update catalog: %v", err)
			}
		}
	}

	err := ret.Write(fs)
	if err != nil {
		log.Printf("Failed to update catalog: %v", err)
	}
	result <- ret
}

func saveCatalog(fs afero.Fs, catalogPath string, items <-chan catalog.Item,
	result chan<- catalog.Catalog, wg *sync.WaitGroup) {
	c := catalog.NewCatalog()
	updateAndSaveCatalog(fs, c, catalogPath, items, result, wg)
}

// Counts the files and sums their sizes in a folder. Only files that pass the filter are counted.
func fileStats(fs afero.Fs, root string, filter FileFilter) (count int64, size int64) {
	if exist, err := afero.DirExists(fs, root); err != nil || !exist {
		log.Fatalf("The folder '%v' doesn't exist", root)
	}
	afero.Walk(fs, root, func(path string, fi os.FileInfo, err error) error {
		if !fi.IsDir() && fi.Name() != catalog.CatalogFileName && filter.Include(fi.Name()) {
			count++
			size += fi.Size()
		}
		return nil
	})
	return
}

// ScanFolder recursively scans the root folder and adds all files to the catalog
func ScanFolder(fs afero.Fs, root string, filter FileFilter) catalog.Catalog {
	fileCount, totalSize := fileStats(fs, root, filter)
	pb := newDoubleProgressBar()
	pb.SetTotal(fileCount, totalSize)

	var wg sync.WaitGroup
	wg.Add(4)
	files := walkFolder(fs, root, &wg)
	filteredFiles := filterFiles(files, filter, &wg)
	items := readCatalogItems(fs, filteredFiles, pb, &wg)
	result := make(chan catalog.Catalog, 1)
	catalogFilePath := filepath.Join(root, catalog.CatalogFileName)
	go saveCatalog(fs, catalogFilePath, items, result, &wg)
	wg.Wait()
	ret := <-result

	pb.Wait()
	return ret
}

// Scan recursively scans the whole file system
func Scan(fs afero.Fs) catalog.Catalog {
	return ScanFolder(fs, ".", noFilter{})
}

// walkDiff gets a set of file paths (as returned by Diff) and return their paths
// the same way as walkFolder
func walkDiff(fs afero.Fs, paths map[string]bool, wg *sync.WaitGroup) chan string {
	files := make(chan string, 100000)
	go func() {
		defer wg.Done()
		for path := range paths {
			_, err := fs.Stat(path)
			if err != nil {
				log.Printf("Cannot read file '%v'", path)
			}
			files <- path
		}
		files <- ""
	}()
	return files
}

// fileStatsFromDiff gets a set of file paths (as returned by Diff) and return the
// count of files and the sum of their sizes, the same way as fileStats
func fileStatsFromDiff(fs afero.Fs, paths map[string]bool) (count int64, size int64) {
	for path := range paths {
		fi, err := fs.Stat(path)
		if err != nil {
			log.Printf("Cannot read file '%v'", path)
		}
		size += fi.Size()
	}
	return int64(len(paths)), size
}

// ScanAdd performs a scan on a folder and checks the contents against a catalog.
// If new files are missing from the catalog they are added and a modified catalog is returned.
func ScanAdd(fs afero.Fs, c catalog.Catalog, diff FileSystemDiff) catalog.Catalog {
	var wg sync.WaitGroup
	fileCount, totalSize := fileStatsFromDiff(fs, diff.Add)
	pb := newDoubleProgressBar()
	pb.SetTotal(fileCount, totalSize)

	wg.Add(3)
	const root = "."
	files := walkDiff(fs, diff.Add, &wg)
	items := readCatalogItems(fs, files, pb, &wg)

	result := make(chan catalog.Catalog, 1)
	catalogFilePath := filepath.Join(root, catalog.CatalogFileName)
	go updateAndSaveCatalog(fs, c, catalogFilePath, items, result, &wg)
	wg.Wait()
	ret := <-result
	pb.Wait()
	return ret
}

// filterByCatalog separate the incoming files (typically contents of the file system)
// to two channels based on whether they are present in the catalog or not.
// If an file read from the files channel is in the catalog (only the path is checked, no metadata, no contents)
// it is put to known otherwise to unknown.
// The processing can be interrupted by a message sent to the done channel.
func filterByCatalog(files <-chan string, c catalog.Catalog, wg *sync.WaitGroup) (known chan string, unknown chan string) {
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

func collectFiles(c <-chan string, m map[string]bool, wg *sync.WaitGroup, label string) {
	defer wg.Done()
	for file := range c {
		if file == "" {
			return
		}
		m[file] = true
	}
}

func collectUnknownFiles(fs afero.Fs, c <-chan string, m map[string]bool, pb DoubleProgressBar, wg *sync.WaitGroup) {
	defer wg.Done()
	for file := range c {
		if file == "" {
			return
		}
		fi, err := fs.Stat(file)
		if err != nil {
			fmt.Printf("Cannot get file size: %v\n", err)
			return
		}
		pb.IncrBy(int(fi.Size()))
		m[file] = true
	}
}

// DiffFiltered scans a folder and compares its contents to the contents of the catalog.
// It performs a full scan and returns the file paths separated into multiple lists based on the file status.
func DiffFiltered(fs afero.Fs, c catalog.Catalog, filter FileFilter, deepCheck bool) FileSystemDiff {
	okFiles := make(chan string, 100)
	changedFiles := make(chan string, 100)
	var wg sync.WaitGroup
	wg.Add(7)

	count, size := fileStats(fs, ".", filter)
	pb := newDoubleProgressBar()
	pb.SetTotal(count, size)

	files := walkFolder(fs, ".", &wg)
	filteredFiles := filterFiles(files, filter, &wg)
	knownFiles, unknownFiles := filterByCatalog(filteredFiles, c, &wg)
	checkExistingItems(fs, deepCheck, knownFiles, c, pb, okFiles, changedFiles, &wg)
	ret := NewFileSystemDiff()

	go collectFiles(okFiles, ret.Ok, &wg, "ok")
	go collectFiles(changedFiles, ret.Update, &wg, "changed")
	go collectUnknownFiles(fs, unknownFiles, ret.Add, pb, &wg)
	wg.Wait()

	pb.Wait()

	for item := range c.AllItems() {
		if item.Path == "" {
			break
		}
		if _, ok := ret.Ok[item.Path]; ok {
			continue
		}
		if _, ok := ret.Update[item.Path]; ok {
			continue
		}
		ret.Delete[item.Path] = true
	}
	return ret
}

// Diff scans a folder and compares it to the catalog the same way as DiffFiltered does but without filtering out any files
func Diff(fs afero.Fs, c catalog.Catalog, deepCheck bool) FileSystemDiff {
	return DiffFiltered(fs, c, noFilter{}, deepCheck)
}
