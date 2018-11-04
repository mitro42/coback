package catalog

import (
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type catalogState = int
type checksum = string

// Catalog stores information about the contents of a folder
type Catalog interface {
	Add(path string) error
	DeletePath(path string)
	Item(path string) (catalogItem, error)
	ItemsByChecksum(sum checksum) ([]catalogItem, error)
	Count() int
	DeletedCount() int
	IsDeletedPath(path string) (bool, error)
	IsDeletedChecksum(sum checksum) (bool, error)
}

type catalog struct {
	State         catalogState  `json:"state"`
	Items         []catalogItem `json:"content"`
	pathToIdx     map[string]int
	checksumToIdx map[string][]int
	fs            afero.Fs
}

func newCatalog(fs afero.Fs) Catalog {
	return &catalog{
		Items:         make([]catalogItem, 0, 100),
		pathToIdx:     make(map[string]int),
		checksumToIdx: make(map[string][]int),
		fs:            fs,
	}
}

func scanFolder(path string) Catalog {
	return &catalog{}
}

func (c *catalog) Add(path string) error {
	item, err := newCatalogItem(c.fs, path)
	if err != nil {
		return errors.Wrap(err, "Cannot add item to catalog")
	}
	c.Items = append(c.Items, *item)
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

func (c *catalog) Item(path string) (catalogItem, error) {
	idx, ok := c.pathToIdx[path]
	if !ok {
		return catalogItem{}, errors.Errorf("No such file: %v", path)
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

func (c *catalog) ItemsByChecksum(sum checksum) ([]catalogItem, error) {
	indexes, ok := c.checksumToIdx[sum]
	if !ok {
		return []catalogItem{}, errors.Errorf("No such file: %v", sum)
	}
	ret := make([]catalogItem, 0, len(indexes))
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
