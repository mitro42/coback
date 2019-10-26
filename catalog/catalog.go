package catalog

import (
	"encoding/json"
	"fmt"
	"sort"

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
	// ForgetPath completely removes and item with the given path from the catalog. It's hash will not be stored as deleted,
	// and other items will not be removed even if they have the same hash.
	ForgetPath(path string)
	// DeletePath removes the path from the Catalog. If there are no other items stored with the same hash, the hash is stored as deleted
	DeletePath(path string)
	// Item returns the Item with the given path. Returns error if the path doesn't exist.
	Item(path string) (Item, error)
	// ItemsByChecksum returns the Items that have the given checksum. Returns error if the no such Item exist.
	// As a copy of the same file can be stored more than once with different paths, a slice of Items is returned,
	// sorted by the path of the items
	ItemsByChecksum(sum Checksum) ([]Item, error)
	// AllItems returns a channel with all the items currently in the Catalog in alphabetical order of the path
	AllItems() <-chan Item
	// DeletedChecksums returns a channel with all the deleted checksums in alphabetical order
	DeletedChecksums() <-chan Checksum
	// Count returns the number of items stored in the Catalog
	Count() int
	// DeletedCount returns the number of items stored in the Catalog which are marked as deleted
	DeletedCount() int
	// DeleteChecksum stores the checksum as deleted and removes all items from the Catalog that have the given checksum (if any)
	DeleteChecksum(sum Checksum)
	// UnDeleteChecksum removes the checksum from the deleted checksums. It does not affect the stored items.
	UnDeleteChecksum(sum Checksum)
	// IsDeletedChecksum returns true all the items with the given checksum are marked as deleted.
	// Returns error if the path doesn't exist or some items are marked as deleted, but not all of them.
	IsDeletedChecksum(sum Checksum) bool
	// IsKnownChecksum returns true is the checksum is in the Catalog, either as an actual item or as a checksum marked as deleted
	IsKnownChecksum(sum Checksum) bool
	// WriteAs writes the Catalog as a file at the given path and file system
	WriteAs(fs afero.Fs, path string) error
	// Write writes the Catalog as 'coback.catalog' in the root of the file system
	Write(fs afero.Fs) error
	// Clone creates a deep copy of the Catalog
	Clone() Catalog
	// FilterNew returns a catalog that contains all items that are present in this Catalog, but not in the other
	// (either as regulas items or deleted hashes)
	FilterNew(other Catalog) Catalog
}

type catalog struct {
	State           catalogState      `json:"state"`
	Items           map[string]Item   `json:"content"`
	Deleted         map[Checksum]bool `json:"deleted_checksums"`
	checksumToPaths map[Checksum][]string
}

func newcatalog() *catalog {
	return &catalog{
		Items:           make(map[string]Item),
		checksumToPaths: make(map[Checksum][]string),
		Deleted:         make(map[Checksum]bool),
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
	for k, v := range c.Deleted {
		clone.Deleted[k] = v
	}
	return clone
}

func (c *catalog) Add(item Item) error {
	if _, ok := c.Items[item.Path]; ok {
		return fmt.Errorf("File is already in the catalog: '%v'", item.Path)
	}

	delete(c.Deleted, item.Md5Sum)
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
		delete(c.Items, item.Path)
		c.removeChecksumToPathMapping(origChecksum, item.Path)
	}
	return c.Add(newItem)
}

func (c *catalog) DeletePath(path string) {
	item, ok := c.Items[path]
	if !ok {
		return
	}
	paths, _ := c.checksumToPaths[item.Md5Sum]
	if len(paths) == 1 {
		c.Deleted[item.Md5Sum] = true
	}
	c.removeChecksumToPathMapping(item.Md5Sum, item.Path)
	delete(c.Items, path)
}

func (c *catalog) DeleteChecksum(sum Checksum) {
	paths, ok := c.checksumToPaths[sum]
	if ok {
		for _, p := range paths {
			item := c.Items[p]
			c.removeChecksumToPathMapping(item.Md5Sum, item.Path)
			delete(c.Items, p)
		}
	}
	c.Deleted[sum] = true
}

func (c *catalog) UnDeleteChecksum(sum Checksum) {
	_, ok := c.Deleted[sum]
	if !ok {
		return
	}
	delete(c.Deleted, sum)
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
	return len(c.Deleted)
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
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Path < ret[j].Path
	})
	return ret, nil
}

func (c *catalog) IsDeletedChecksum(sum Checksum) bool {
	deleted, ok := c.Deleted[sum]
	return ok && deleted
}

func (c *catalog) Write(fs afero.Fs) error {
	return c.WriteAs(fs, CatalogFileName)
}

func (c *catalog) WriteAs(fs afero.Fs, path string) error {
	json, _ := json.Marshal(c)
	err := afero.WriteFile(fs, path, json, 0644)
	return errors.Wrapf(err, "Cannot save catalog to file: '%v'", path)
}

func (c *catalog) FilterNew(other Catalog) Catalog {
	ret := NewCatalog()
	for _, item := range c.Items {
		if other.IsDeletedChecksum(item.Md5Sum) {
			continue
		}
		if _, err := other.ItemsByChecksum(item.Md5Sum); err == nil {
			continue
		}
		ret.Add(item)
	}
	return ret
}

func (c *catalog) IsKnownChecksum(sum Checksum) bool {
	_, ok := c.checksumToPaths[sum]
	if ok {
		return true
	}
	_, ok = c.Deleted[sum]
	if ok {
		return true
	}
	return false
}

func (c *catalog) removeChecksumToPathMapping(sum Checksum, path string) {
	if len(c.checksumToPaths[sum]) == 1 {
		delete(c.checksumToPaths, sum)
	} else {
		c.checksumToPaths[sum] = removeItem(c.checksumToPaths[sum], path)
	}
}

func (c *catalog) ForgetPath(path string) {
	item, ok := c.Items[path]
	if !ok {
		return
	}
	c.removeChecksumToPathMapping(item.Md5Sum, item.Path)
	delete(c.Items, item.Path)
}

func (c *catalog) AllItems() <-chan Item {
	ret := make(chan Item, 100)
	go func() {
		defer func() {
			ret <- Item{}
			close(ret)
		}()

		if len(c.Items) == 0 {
			return
		}
		keys := make([]string, 0, len(c.Items))
		for _, item := range c.Items {
			keys = append(keys, item.Path)
		}

		sort.Strings(keys)

		for _, k := range keys {
			ret <- c.Items[k]
		}

	}()

	return ret
}

func (c *catalog) DeletedChecksums() <-chan Checksum {
	ret := make(chan Checksum, 100)
	keys := make([]Checksum, 0, len(c.Deleted))
	for checksum := range c.Deleted {
		keys = append(keys, checksum)
	}
	go func() {
		defer close(ret)

		if len(keys) == 0 {
			return
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})

		for _, k := range keys {
			ret <- k
		}

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
