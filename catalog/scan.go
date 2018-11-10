package catalog

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/afero"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

func walkFolder(fs afero.Fs, root string) (<-chan string, <-chan int64) {
	files := make(chan string, 100000)
	sizes := make(chan int64, 100000)
	if exist, err := afero.DirExists(fs, root); err != nil || !exist {
		log.Fatalf("The folder '%v' doesn't exist", root)
	}
	go func() {
		afero.Walk(fs, root, func(path string, fi os.FileInfo, err error) error {
			if !fi.IsDir() {
				files <- path
				sizes <- fi.Size()
			}
			return nil
		})
		close(files)
		close(sizes)
	}()
	return files, sizes
}

func catalogFile(fs afero.Fs, path string, out chan CatalogItem, countBar *mpb.Bar, sizeBar *mpb.Bar) {
	start := time.Now()
	item, err := newCatalogItem(fs, path)
	if err != nil {
		log.Printf("Cannot read file '%v'", path)
	} else {
		sizeBar.IncrBy(int(item.Size), time.Since(start))
		countBar.IncrBy(1, time.Since(start))
		out <- *item
	}
}

func readCatalogItems(fs afero.Fs, paths <-chan string, countBar *mpb.Bar, sizeBar *mpb.Bar) <-chan CatalogItem {
	out := make(chan CatalogItem)
	var wg sync.WaitGroup
	const concurrency = 6
	wg.Add(concurrency)
	go func() {
		for i := 0; i < concurrency; i++ {
			go func() {
				for path := range paths {
					catalogFile(fs, path, out, countBar, sizeBar)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		close(out)
		sizeBar.SetTotal(sizeBar.Current(), true)
		countBar.SetTotal(countBar.Current(), true)
	}()

	return out
}

func sumSizes(sizes <-chan int64, countBar *mpb.Bar, sizeBar *mpb.Bar) {
	total := int64(0)
	count := int64(0)
	for s := range sizes {
		total += s
		count++
		sizeBar.SetTotal(total, false)
		countBar.SetTotal(count, false)
	}
}

func createProgressBars() (*mpb.Progress, *mpb.Bar, *mpb.Bar) {
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

func saveCatalog(fs afero.Fs, catalogPath string, items <-chan CatalogItem, result chan<- Catalog) {
	lastSave := time.Now()
	c := NewCatalog()
	for item := range items {
		c.Add(item)
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
func ScanFolder(fs afero.Fs, root string) Catalog {

	p, countBar, sizeBar := createProgressBars()

	files, sizes := walkFolder(fs, root)
	items := readCatalogItems(fs, files, countBar, sizeBar)
	go sumSizes(sizes, countBar, sizeBar)
	result := make(chan Catalog)
	catalogFilePath := filepath.Join(root, "coback.catalog")
	go saveCatalog(fs, catalogFilePath, items, result)
	p.Wait()
	log.Print("Scanning done")
	return <-result
}

// Scan recursively scans the whole file system
func Scan(fs afero.Fs) Catalog {
	return ScanFolder(fs, ".")
}
