package catalog

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// CatalogFileName is the file where coback stores the catalog in json format
const CatalogFileName = "coback.catalog"

type catalogState = int
type checksum = string

// Catalog stores information about the contents of a folder
type Catalog interface {
	Add(item Item) error
	Set(item Item) error
	DeletePath(path string)
	Item(path string) (Item, error)
	ItemsByChecksum(sum checksum) ([]Item, error)
	Count() int
	DeletedCount() int
	IsDeletedPath(path string) (bool, error)
	IsDeletedChecksum(sum checksum) (bool, error)
	Write(fs afero.Fs, path string) error
	Clone() Catalog
}

type catalog struct {
	State         catalogState `json:"state"`
	Items         []Item       `json:"content"`
	pathToIdx     map[string]int
	checksumToIdx map[string][]int
}

// NewCatalog creates a new empty Catalog
func NewCatalog() Catalog {
	return &catalog{
		Items:         make([]Item, 0, 100),
		pathToIdx:     make(map[string]int),
		checksumToIdx: make(map[string][]int),
	}
}

func (c *catalog) Clone() Catalog {
	clone := &catalog{
		Items:         make([]Item, len(c.Items)),
		pathToIdx:     make(map[string]int),
		checksumToIdx: make(map[string][]int),
	}
	copy(clone.Items, c.Items)
	for k, v := range c.pathToIdx {
		clone.pathToIdx[k] = v
	}
	for k, v := range c.checksumToIdx {
		clone.checksumToIdx[k] = v
	}
	return clone
}

func (c *catalog) Add(item Item) error {
	if _, ok := c.pathToIdx[item.Path]; ok {
		return fmt.Errorf("File is already in the catalog: '%v'", item.Path)
	}
	c.Items = append(c.Items, item)
	idx := len(c.Items) - 1
	c.pathToIdx[item.Path] = idx
	c.checksumToIdx[item.Md5Sum] = append(c.checksumToIdx[item.Md5Sum], idx)
	return nil
}

// removeItem removes the first occurrence of a value from a slice
func removeItem(slice []int, v int) []int {
	for idx, item := range slice {
		if item == v {
			return append(slice[:idx], slice[idx+1:]...)
		}
	}
	return slice
}

func (c *catalog) Set(item Item) error {
	if idx, ok := c.pathToIdx[item.Path]; ok {
		origChecksum := c.Items[idx].Md5Sum
		c.checksumToIdx[origChecksum] = removeItem(c.checksumToIdx[origChecksum], idx)
		c.checksumToIdx[item.Md5Sum] = append(c.checksumToIdx[item.Md5Sum], idx)
		c.Items[idx] = item
		return nil
	}
	return c.Add(item)
}

func (c *catalog) DeletePath(path string) {
	idx, ok := c.pathToIdx[path]
	if ok {
		c.Items[idx].Deleted = true
	}
}

func (c *catalog) Item(path string) (Item, error) {
	idx, ok := c.pathToIdx[path]
	if !ok {
		return Item{}, errors.Errorf("No such file: %v", path)
	}
	return c.Items[idx], nil
}

func (c *catalog) Count() int {
	return len(c.Items) - c.DeletedCount()
}

func (c *catalog) DeletedCount() int {
	count := 0
	for _, item := range c.Items {
		if item.Deleted {
			count++
		}
	}
	return count
}

func (c *catalog) ItemsByChecksum(sum checksum) ([]Item, error) {
	indexes, ok := c.checksumToIdx[sum]
	if !ok {
		return []Item{}, errors.Errorf("No such file: %v", sum)
	}
	ret := make([]Item, 0, len(indexes))
	for idx := range indexes {
		ret = append(ret, c.Items[idx])
	}
	return ret, nil
}

func (c *catalog) areAllDeleted(indexes []int) (bool, error) {
	switch len(indexes) {
	case 0:
		return false, errors.New("No indexes")
	case 1:
		return c.Items[indexes[0]].Deleted, nil
	default:
		for idx := range indexes {
			if c.Items[idx].Deleted != c.Items[0].Deleted {
				return false, errors.Errorf("Some items are deleted, some are not! Indexes: %v", indexes)
			}
		}
		return c.Items[indexes[0]].Deleted, nil
	}
}

func (c *catalog) IsDeletedPath(path string) (bool, error) {
	idx, ok := c.pathToIdx[path]
	if !ok {
		return false, errors.Errorf("No such file: %v", path)
	}
	return c.Items[idx].Deleted, nil
}

func (c *catalog) IsDeletedChecksum(sum checksum) (bool, error) {
	indexes, ok := c.checksumToIdx[sum]
	if !ok {
		return false, errors.Errorf("No such file: %v", sum)
	}

	return c.areAllDeleted(indexes)
}

func (c *catalog) Write(fs afero.Fs, path string) error {
	json, _ := json.Marshal(c)
	err := afero.WriteFile(fs, path, json, 0644)
	return errors.Wrapf(err, "Cannot save catalog to file: '%v'", path)
}

// Read reads catalog stored in a json file
func Read(fs afero.Fs, path string) (Catalog, error) {
	buf, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot read catalog: '%v'", path)
	}
	c := &catalog{
		Items:         make([]Item, 0),
		pathToIdx:     make(map[string]int),
		checksumToIdx: make(map[string][]int),
	}
	err = json.Unmarshal(buf, c)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot parse catalog json: '%v'", path)
	}

	for idx, item := range c.Items {
		c.pathToIdx[item.Path] = idx
		c.checksumToIdx[item.Md5Sum] = append(c.checksumToIdx[item.Md5Sum], idx)
	}
	return c, nil
}
