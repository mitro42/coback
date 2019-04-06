package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestRemoveItem(t *testing.T) {
	th.Equals(t, []int{}, removeItem([]int{}, 42))
	th.Equals(t, []int{}, removeItem([]int{42}, 42))
	th.Equals(t, []int{4}, removeItem([]int{4}, 42))
	th.Equals(t, []int{4}, removeItem([]int{0, 4}, 0))
	th.Equals(t, []int{4}, removeItem([]int{4, 0}, 0))
	th.Equals(t, []int{2, 3, 4}, removeItem([]int{88, 2, 3, 4}, 88))
	th.Equals(t, []int{2, 3, 4}, removeItem([]int{2, 88, 3, 4}, 88))
	th.Equals(t, []int{2, 3, 4}, removeItem([]int{2, 3, 88, 4}, 88))
	th.Equals(t, []int{2, 3, 4}, removeItem([]int{2, 3, 4, 88}, 88))
	th.Equals(t, []int{2, 2}, removeItem([]int{2, 2, 2}, 2))
	th.Equals(t, []int{2, 2}, removeItem([]int{2, 3, 2}, 3))
	th.Equals(t, []int{3, 2}, removeItem([]int{2, 3, 2}, 2))
}

func TestEmptyCatalog(t *testing.T) {
	c := NewCatalog()
	th.Equals(t, c.Count(), 0)
	th.Equals(t, c.DeletedCount(), 0)
}

func TestEmptyCatalogAddRetrieve(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	expectedItem, err := newItem(fs, path)
	th.Ok(t, err)
	c := NewCatalog()
	err = c.Add(*expectedItem)
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
	th.Equals(t, []Item{*expectedItem}, items)

	path2 := "test_data/test2.txt"
	_, err = c.Item(path2)
	th.Nok(t, err, "No such file: "+path2)

	items, err = c.ItemsByChecksum("89b2b34c7b8d232041f0fcc1d213d7bc")
	th.Nok(t, err, "No such file: 89b2b34c7b8d232041f0fcc1d213d7bc")
	th.Equals(t, []Item{}, items)
}

func TestAddExisting(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	c := NewCatalog()
	item, _ := newItem(fs, path)
	err := c.Add(*item)
	th.Ok(t, err)
	err = c.Add(*item)
	th.NokPrefix(t, err, "File is already in the catalog")
	th.Equals(t, 1, c.Count())
	th.Equals(t, 0, c.DeletedCount())
}

func TestAddDelete(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	c := NewCatalog()
	expectedItem, err := newItem(fs, path)
	th.Ok(t, err)
	err = c.Add(*expectedItem)
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
	checksum2 := Checksum("89b2b34c7b8d232041f0fcc1d213d7bc")
	_, err = c.Item(path2)
	th.Nok(t, err, "No such file: "+path2)

	items, err := c.ItemsByChecksum(checksum2)
	th.Nok(t, err, fmt.Sprintf("No such file: %v", checksum2))
	th.Equals(t, []Item{}, items)

	_, err = c.IsDeletedPath(path2)
	th.NokPrefix(t, err, "No such file: "+path2)

	_, err = c.IsDeletedChecksum(checksum2)
	th.NokPrefix(t, err, fmt.Sprintf("No such file: %v", checksum2))
}

func TestSetMissing(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	c := NewCatalog()
	expectedItem, _ := newItem(fs, path)
	err := c.Set(*expectedItem)
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
	checksum2 := Checksum("89b2b34c7b8d232041f0fcc1d213d7bc")
	_, err = c.Item(path2)
	th.Nok(t, err, "No such file: "+path2)

	items, err := c.ItemsByChecksum(checksum2)
	th.Nok(t, err, fmt.Sprintf("No such file: %v", checksum2))
	th.Equals(t, []Item{}, items)

	_, err = c.IsDeletedPath(path2)
	th.NokPrefix(t, err, "No such file: "+path2)

	_, err = c.IsDeletedChecksum(checksum2)
	th.NokPrefix(t, err, fmt.Sprintf("No such file: %v", checksum2))
}

func TestSetExisting(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	c := NewCatalog()
	item, _ := newItem(fs, path)
	err := c.Add(*item)
	th.Ok(t, err)

	other := *item
	other.Md5Sum = "x"
	other.Size = 12345
	other.ModificationTime = "yesterday"
	th.Equals(t, 1, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	actual, _ := c.Item(item.Path)
	th.Equals(t, *item, actual)
	actualList, _ := c.ItemsByChecksum(item.Md5Sum)
	th.Equals(t, []Item{*item}, actualList)
	c.Set(other)
	th.Equals(t, 1, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	actual, _ = c.Item(item.Path)
	th.Equals(t, other, actual)
	actualList, _ = c.ItemsByChecksum(other.Md5Sum)
	th.Equals(t, []Item{other}, actualList)
	actualList, _ = c.ItemsByChecksum(item.Md5Sum)
	th.Equals(t, []Item{}, actualList)
}

func TestReadMissing(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data/subfolder"
	fs := createSafeFs(filepath.Join(basePath, path))
	c, err := Read(fs, CatalogFileName)
	th.NokPrefix(t, err, "Cannot read catalog: 'coback.catalog'")
	th.Equals(t, c, nil)
}

func TestReadParseError(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data/subfolder"
	fs := createSafeFs(filepath.Join(basePath, path))
	afero.WriteFile(fs, CatalogFileName, []byte("Not a valid json"), 0644)
	c, err := Read(fs, CatalogFileName)
	th.NokPrefix(t, err, "Cannot parse catalog json: 'coback.catalog'")
	th.Equals(t, c, nil)
}

func TestWriteReadOneLevel(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data/subfolder"
	fs := createSafeFs(filepath.Join(basePath, path))
	c := Scan(fs)
	c2, err := Read(fs, CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, c, c2)
}

func TestWriteReadRecursive(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	c := Scan(fs)
	c2, err := Read(fs, CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, c, c2)
}

func TestClone(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := createSafeFs(filepath.Join(basePath, path))
	c := Scan(fs).(*catalog)
	clone := c.Clone().(*catalog)
	th.Equals(t, c, clone)
	th.Assert(t, &c != &clone, "clone should be a different object")
	th.Equals(t, c.Items, clone.Items)
	th.Assert(t, &c.Items != &clone.Items, "clone.Items should be a different object")
	th.Equals(t, c.pathToIdx, clone.pathToIdx)
	th.Assert(t, &c.pathToIdx != &clone.pathToIdx, "clone.pathToIdx should be a different object")
	th.Equals(t, c.checksumToIdx, clone.checksumToIdx)
	th.Assert(t, &c.checksumToIdx != &clone.checksumToIdx, "clone.checksumToIdx should be a different object")
}
