package catalog

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// CatalogFileName is the file where coback stores the catalog in json format
const CatalogFileName = "coback.catalog"

type catalogState int

// Checksum is a string type that helps avoiding confusion between files identified by path and by checksum
type Checksum string

// Catalog stores information about the contents of a folder
type Catalog interface {
	// Add adds a new items to the Catalog
	Add(item Item) error
	// Set ensures that the item in the catalog with the given path contains the given data.
	// If an item with the same path already exists it is replaced with the new item.
	// Simply adds the item if the path it is not yet in the Catalog.
	Set(item Item) error
	// DeletePath marks the item with the given path as deleted. No error if the path doesn't exists in the Catalog
	DeletePath(path string) error
	// Item returns the Item with the given path. Returns error if the path doesn't exist.
	Item(path string) (Item, error)
	// ItemsByChecksum returns the Items that have the given checksum. Returns error if the no such Item exist.
	// As a copy of the same file can be stored more than once with different paths, a slice of Items is returned.
	ItemsByChecksum(sum Checksum) ([]Item, error)
	// AllItems returns a channel with all the items currently in the Catalog
	AllItems() <-chan Item
	// Count returns the number of items stored in the Catalog
	Count() int
	// DeletedCount returns the number of items stored in the Catalog which are marked as deleted
	DeletedCount() int
	// IsDeletedChecksum returns true all the items with the given checksum are marked as deleted.
	// Returns error if the path doesn't exist or some items are marked as deleted, but not all of them.
	IsDeletedChecksum(sum Checksum) bool
	// Write writes the Catalog as a file at the given path and file system
	Write(fs afero.Fs, path string) error
	// Clone creates a deep copy of the Catalog
	Clone() Catalog
	// FilterNew returns a catalog that contains all items that are present in this Catalog, but not in the other
	FilterNew(other Catalog) Catalog
}

type catalog struct {
	State            catalogState      `json:"state"`
	Items            map[string]Item   `json:"content"`
	DeletedChecksums map[Checksum]bool `json:"deleted_checksums"`
	checksumToPaths  map[Checksum][]string
}

func newcatalog() *catalog {
	return &catalog{
		Items:            make(map[string]Item),
		checksumToPaths:  make(map[Checksum][]string),
		DeletedChecksums: make(map[Checksum]bool),
	}
}

// NewCatalog creates a new empty Catalog
func NewCatalog() Catalog {
	return newcatalog()
}

func (c *catalog) Clone() Catalog {
	clone := newcatalog()
	for k, v := range c.Items {
		clone.Items[k] = v
	}
	for k, v := range c.checksumToPaths {
		clone.checksumToPaths[k] = v
	}
	for k, v := range c.DeletedChecksums {
		clone.DeletedChecksums[k] = v
	}
	return clone
}

func (c *catalog) Add(item Item) error {
	if _, ok := c.Items[item.Path]; ok {
		return fmt.Errorf("File is already in the catalog: '%v'", item.Path)
	}
	if c.DeletedChecksums[item.Md5Sum] {
		return fmt.Errorf("The hash of the file is stored as deleted: '%v'", item.Path)
	}

	c.Items[item.Path] = item
	c.checksumToPaths[item.Md5Sum] = append(c.checksumToPaths[item.Md5Sum], item.Path)

	return nil
}

// removeItem removes the first occurrence of a value from a slice
func removeItem(slice []string, v string) []string {
	for idx, item := range slice {
		if item == v {
			return append(slice[:idx], slice[idx+1:]...)
		}
	}
	return slice
}

func (c *catalog) Set(newItem Item) error {
	if item, ok := c.Items[newItem.Path]; ok {
		origChecksum := c.Items[item.Path].Md5Sum
		c.checksumToPaths[origChecksum] = removeItem(c.checksumToPaths[origChecksum], item.Path)
		c.checksumToPaths[newItem.Md5Sum] = append(c.checksumToPaths[newItem.Md5Sum], newItem.Path)
		c.Items[item.Path] = newItem
		return nil
	}
	return c.Add(newItem)
}

func (c *catalog) DeletePath(path string) error {
	item, ok := c.Items[path]
	if !ok {
		return nil
	}
	paths, ok := c.checksumToPaths[item.Md5Sum]
	if !ok {
		return errors.Errorf("Inconsistent catalog, cannot map checksum to path: '%s', '%s'", path, item.Md5Sum)
	}
	if len(paths) == 1 {
		c.DeletedChecksums[item.Md5Sum] = true
	}
	delete(c.Items, path)
	return nil
}

func (c *catalog) DeleteChecksum(sum Checksum) {
	paths, ok := c.checksumToPaths[sum]
	if ok {
		for _, p := range paths {
			delete(c.Items, p)
		}
		c.DeletedChecksums[sum] = true
	}
}

func (c *catalog) Item(path string) (Item, error) {
	item, ok := c.Items[path]
	if !ok {
		return Item{}, errors.Errorf("No such file: %v", path)
	}
	return item, nil
}

func (c *catalog) Count() int {
	return len(c.Items)
}

func (c *catalog) DeletedCount() int {
	return len(c.DeletedChecksums)
}

func (c *catalog) ItemsByChecksum(sum Checksum) ([]Item, error) {
	paths, ok := c.checksumToPaths[sum]
	if !ok {
		return []Item{}, errors.Errorf("No such file: %v", sum)
	}
	ret := make([]Item, 0, len(paths))
	for _, path := range paths {
		ret = append(ret, c.Items[path])
	}
	return ret, nil
}

// func (c *catalog) areAllDeleted(indexes []int) (bool, error) {
// 	switch len(indexes) {
// 	case 0:
// 		return false, errors.New("No indexes")
// 	case 1:
// 		return c.Items[indexes[0]].Deleted, nil
// 	default:
// 		for idx := range indexes {
// 			if c.Items[idx].Deleted != c.Items[0].Deleted {
// 				return false, errors.Errorf("Some items are deleted, some are not! Indexes: %v", indexes)
// 			}
// 		}
// 		return c.Items[indexes[0]].Deleted, nil
// 	}
// }

// func (c *catalog) IsDeletedPath(path string) (bool, error) {
// 	deleted, ok := c.pathToIdx[path]
// 	if !ok {
// 		return false, errors.Errorf("No such file: %v", path)
// 	}
// 	return c.Items[idx].Deleted, nil
// }

func (c *catalog) IsDeletedChecksum(sum Checksum) bool {
	deleted, ok := c.DeletedChecksums[sum]
	return ok && deleted
}

func (c *catalog) Write(fs afero.Fs, path string) error {
	json, _ := json.Marshal(c)
	err := afero.WriteFile(fs, path, json, 0644)
	return errors.Wrapf(err, "Cannot save catalog to file: '%v'", path)
}

func (c *catalog) FilterNew(other Catalog) Catalog {
	ret := NewCatalog()
	for _, item := range c.Items {
		if _, err := other.ItemsByChecksum(item.Md5Sum); err != nil {
			ret.Add(item)
		}
	}
	return ret
}

func (c *catalog) AllItems() <-chan Item {
	ret := make(chan Item, 100)
	go func() {
		for _, item := range c.Items {
			ret <- item
		}
		ret <- Item{}
		close(ret)
	}()

	return ret
}

// Read reads catalog stored in a json file
func Read(fs afero.Fs, path string) (Catalog, error) {
	buf, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot read catalog: '%v'", path)
	}
	c := newcatalog()
	err = json.Unmarshal(buf, c)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot parse catalog json: '%v'", path)
	}

	for _, item := range c.Items {
		c.checksumToPaths[item.Md5Sum] = append(c.checksumToPaths[item.Md5Sum], item.Path)
	}
	return c, nil
}
