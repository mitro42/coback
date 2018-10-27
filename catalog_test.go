package main

import (
	"testing"

	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestEmptyCatalog(t *testing.T) {
	fs := afero.NewOsFs()
	c := newCatalog(fs)
	th.Equals(t, c.Count(), 0)
	th.Equals(t, c.DeletedCount(), 0)
}

func TestEmptyCatalogAddRetrieve(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	expectedItem, err := newCatalogItem(fs, path)
	th.Ok(t, err)
	c := newCatalog(fs)
	err = c.Add(path)
	th.Ok(t, err)

	deleted, err := c.IsDeletedPath(path)
	th.Ok(t, err)
	th.Equals(t, false, deleted)

	deleted, err = c.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117")
	th.Ok(t, err)
	th.Equals(t, false, deleted)

	item, err := c.Item(path)
	th.Ok(t, err)
	th.Equals(t, *expectedItem, item)

	items, err := c.ItemsByChecksum("b3cd1cf6179bca32fd5d76473b129117")
	th.Ok(t, err)
	th.Equals(t, []catalogItem{*expectedItem}, items)

	path2 := "test_data/test2.txt"
	_, err = c.Item(path2)
	th.Nok(t, err, "No such file: "+path2)

	items, err = c.ItemsByChecksum("89b2b34c7b8d232041f0fcc1d213d7bc")
	th.Nok(t, err, "No such file: 89b2b34c7b8d232041f0fcc1d213d7bc")
	th.Equals(t, []catalogItem{}, items)
}

func TestAddDelete(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	c := newCatalog(fs)

	err := c.Add(path)
	th.Ok(t, err)
	th.Equals(t, 1, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	c.DeletePath(path)
	th.Equals(t, 1, c.DeletedCount())
	th.Equals(t, 0, c.Count())

	deleted, err := c.IsDeletedPath(path)
	th.Ok(t, err)
	th.Equals(t, true, deleted)

	deleted, err = c.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117")
	th.Ok(t, err)
	th.Equals(t, true, deleted)

	path2 := "test_data/test2.txt"
	checksum2 := "89b2b34c7b8d232041f0fcc1d213d7bc"
	_, err = c.Item(path2)
	th.Nok(t, err, "No such file: "+path2)

	items, err := c.ItemsByChecksum(checksum2)
	th.Nok(t, err, "No such file: "+checksum2)
	th.Equals(t, []catalogItem{}, items)

	_, err = c.IsDeletedPath(path2)
	th.NokPrefix(t, err, "No such file: "+path2)

	_, err = c.IsDeletedChecksum(checksum2)
	th.NokPrefix(t, err, "No such file: "+checksum2)
}
