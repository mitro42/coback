package catalog

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type catalogState = int
type checksum = string

// Catalog stores information about the contents of a folder
type Catalog interface {
	Add(item CatalogItem) error
	DeletePath(path string)
	Item(path string) (CatalogItem, error)
	ItemsByChecksum(sum checksum) ([]CatalogItem, error)
	Count() int
	DeletedCount() int
	IsDeletedPath(path string) (bool, error)
	IsDeletedChecksum(sum checksum) (bool, error)
	Write(fs afero.Fs, path string) error
}

type catalog struct {
	State         catalogState  `json:"state"`
	Items         []CatalogItem `json:"content"`
	pathToIdx     map[string]int
	checksumToIdx map[string][]int
}

func NewCatalog() Catalog {
	return &catalog{
		Items:         make([]CatalogItem, 0, 100),
		pathToIdx:     make(map[string]int),
		checksumToIdx: make(map[string][]int),
	}
}

func (c *catalog) Add(item CatalogItem) error {
	c.Items = append(c.Items, item)
	idx := len(c.Items) - 1
	c.pathToIdx[item.Path] = idx
	c.checksumToIdx[item.Md5Sum] = append(c.checksumToIdx[item.Md5Sum], idx)
	return nil
}

func (c *catalog) DeletePath(path string) {
	idx, ok := c.pathToIdx[path]
	if ok {
		c.Items[idx].Deleted = true
	}
}

func (c *catalog) Item(path string) (CatalogItem, error) {
	idx, ok := c.pathToIdx[path]
	if !ok {
		return CatalogItem{}, errors.Errorf("No such file: %v", path)
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

func (c *catalog) ItemsByChecksum(sum checksum) ([]CatalogItem, error) {
	indexes, ok := c.checksumToIdx[sum]
	if !ok {
		return []CatalogItem{}, errors.Errorf("No such file: %v", sum)
	}
	ret := make([]CatalogItem, 0, len(indexes))
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

func Read(fs afero.Fs, path string) (Catalog, error) {
	buf, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, errors.Wrapf(err, "Cannot read catalog: '%v'", path)
	}
	c := &catalog{
		Items:         make([]CatalogItem, 0),
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
