package catalog

import (
	"log"
	"os"

	"github.com/spf13/afero"
)

func walkFolder(fs afero.Fs, root string) (<-chan string, <-chan int64) {
	files := make(chan string)
	sizes := make(chan int64)
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

func readCatalogItems(fs afero.Fs, root string, paths <-chan string) <-chan CatalogItem {
	out := make(chan CatalogItem)
	go func() {
		for path := range paths {
			item, err := newCatalogItem(fs, path)
			if err != nil {
				log.Printf("Cannot read file '%v'", path)
			} else {
				log.Printf("Processing file '%v'", path)
				out <- *item
			}
		}
		close(out)
	}()
	return out
}

func sumSizes(sizes <-chan int64) {
	total := int64(0)
	for s := range sizes {
		total += s
		log.Printf("Total size: %v MB", total/1024/1024)
	}
}

// ScanFolder recursively scans the root folder and adds all files to the catalog
func ScanFolder(c Catalog, fs afero.Fs, root string) {
	files, sizes := walkFolder(fs, root)
	items := readCatalogItems(fs, root, files)
	go sumSizes(sizes)
	for item := range items {
		c.Add(item)
	}
}
