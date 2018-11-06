package catalog

import (
	"log"
	"os"
	"time"

	"github.com/spf13/afero"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

func walkFolder(fs afero.Fs, root string) (<-chan string, <-chan int64) {
	files := make(chan string, 100000)
	sizes := make(chan int64, 100000)
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

func readCatalogItems(fs afero.Fs, root string, paths <-chan string, countBar *mpb.Bar, sizeBar *mpb.Bar) <-chan CatalogItem {
	out := make(chan CatalogItem)
	go func() {
		for path := range paths {
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

// ScanFolder recursively scans the root folder and adds all files to the catalog
func ScanFolder(c Catalog, fs afero.Fs, root string) {
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

	files, sizes := walkFolder(fs, root)
	items := readCatalogItems(fs, root, files, countBar, sizeBar)
	go sumSizes(sizes, countBar, sizeBar)
	for item := range items {
		c.Add(item)
	}
	p.Wait()
	log.Print("All done")
}
