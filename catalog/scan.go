package catalog

import (
	"log"
	"os"

	"github.com/spf13/afero"
)

// ScanFolder recursively scans the root folder and adds all files to the catalog
func ScanFolder(c Catalog, fs afero.Fs, root string) {
	afero.Walk(fs, root, func(path string, fi os.FileInfo, err error) error {
		if !fi.IsDir() {
			item, err := newCatalogItem(fs, path)
			if err != nil {
				log.Printf("Cannot read file '%v'", path)
			} else {
				log.Printf("Processing file '%v'", path)
				c.Add(*item)
			}
		}
		return nil
	})

}
