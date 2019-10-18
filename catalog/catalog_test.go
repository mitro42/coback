package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	cth "github.com/mitro42/coback/catalogtesthelper"
	fsh "github.com/mitro42/coback/fshelper"
	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestRemoveItem(t *testing.T) {
	th.Equals(t, []string{}, removeItem([]string{}, "42"))
	th.Equals(t, []string{}, removeItem([]string{"42"}, "42"))
	th.Equals(t, []string{"4"}, removeItem([]string{"4"}, "42"))
	th.Equals(t, []string{"4"}, removeItem([]string{"0", "4"}, "0"))
	th.Equals(t, []string{"4"}, removeItem([]string{"4", "0"}, "0"))
	th.Equals(t, []string{"2", "3", "4"}, removeItem([]string{"88", "2", "3", "4"}, "88"))
	th.Equals(t, []string{"2", "3", "4"}, removeItem([]string{"2", "88", "3", "4"}, "88"))
	th.Equals(t, []string{"2", "3", "4"}, removeItem([]string{"2", "3", "88", "4"}, "88"))
	th.Equals(t, []string{"2", "3", "4"}, removeItem([]string{"2", "3", "4", "88"}, "88"))
	th.Equals(t, []string{"2", "2"}, removeItem([]string{"2", "2", "2"}, "2"))
	th.Equals(t, []string{"2", "2"}, removeItem([]string{"2", "3", "2"}, "3"))
	th.Equals(t, []string{"3", "2"}, removeItem([]string{"2", "3", "2"}, "2"))
}

func TestEmptyCatalog(t *testing.T) {
	c := NewCatalog()
	th.Equals(t, c.Count(), 0)
	th.Equals(t, c.DeletedCount(), 0)
}

func TestEmptyCatalogAddRetrieve(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	expectedItem, err := NewItem(fs, path)
	th.Ok(t, err)
	c := NewCatalog()
	err = c.Add(*expectedItem)
	th.Ok(t, err)

	deleted := c.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117")
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
	item, _ := NewItem(fs, path)
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
	c.DeletePath(path)
	th.Equals(t, 0, c.Count())
	th.Equals(t, 0, c.DeletedCount())

	expectedItem, err := NewItem(fs, path)
	th.Ok(t, err)
	err = c.Add(*expectedItem)
	th.Ok(t, err)
	th.Equals(t, 1, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	c.DeletePath(path)
	th.Equals(t, 1, c.DeletedCount())
	th.Equals(t, 0, c.Count())

	deleted := c.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117")
	th.Equals(t, true, deleted)

	path2 := "test_data/test2.txt"
	checksum2 := Checksum("89b2b34c7b8d232041f0fcc1d213d7bc")
	_, err = c.Item(path2)
	th.Nok(t, err, "No such file: "+path2)

	items, err := c.ItemsByChecksum(checksum2)
	th.Nok(t, err, fmt.Sprintf("No such file: %v", checksum2))
	th.Equals(t, []Item{}, items)
}

func TestDeleteChecksum(t *testing.T) {
	fs := afero.NewOsFs()
	const sum1 = "12345"
	c := NewCatalog()

	c.DeleteChecksum(sum1)
	th.Equals(t, true, c.IsDeletedChecksum(sum1))
	th.Equals(t, 1, c.DeletedCount())

	path := "test_data/test1.txt"
	item, err := NewItem(fs, path)
	th.Ok(t, err)
	err = c.Add(*item)
	sum2 := item.Md5Sum
	th.Ok(t, err)
	th.Equals(t, 1, c.Count())
	th.Equals(t, 1, c.DeletedCount())
	th.Equals(t, false, c.IsDeletedChecksum(sum2))

	c.DeleteChecksum(sum2)
	storedItem, err := c.Item(path)
	th.NokPrefix(t, err, "No such file")
	th.Equals(t, Item{}, storedItem)
	th.Equals(t, 0, c.Count())
	th.Equals(t, 2, c.DeletedCount())
	th.Equals(t, true, c.IsDeletedChecksum(sum2))
}

func TestDeletePathWithDuplicatedChecksum(t *testing.T) {
	fs := afero.NewOsFs()
	c := NewCatalog()

	path1 := "test_data/test1.txt"
	item1, err := NewItem(fs, path1)
	th.Ok(t, err)
	path2 := "test_data/test2.txt"
	item2, err := NewItem(fs, path2)
	item2.Md5Sum = item1.Md5Sum
	th.Ok(t, err)
	err = c.Add(*item1)
	th.Ok(t, err)
	err = c.Add(*item2)
	th.Ok(t, err)
	th.Equals(t, 2, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	th.Equals(t, false, c.IsDeletedChecksum(item1.Md5Sum))

	c.DeletePath(item1.Path)
	storedItem, err := c.Item(item1.Path)
	th.NokPrefix(t, err, "No such file")
	th.Equals(t, Item{}, storedItem)
	storedItem, err = c.Item(item2.Path)
	th.Ok(t, err)
	th.Equals(t, *item2, storedItem)

	c.DeletePath(item2.Path)
	th.Equals(t, 0, c.Count())
	th.Equals(t, 1, c.DeletedCount())
	th.Equals(t, true, c.IsDeletedChecksum(item1.Md5Sum))
}

func TestAddFileWithDeletedChecksum(t *testing.T) {
	fs := afero.NewOsFs()
	c := NewCatalog()

	path := "test_data/test1.txt"
	item, err := NewItem(fs, path)
	th.Ok(t, err)

	c.DeleteChecksum(item.Md5Sum)
	th.Equals(t, 0, c.Count())
	th.Equals(t, 1, c.DeletedCount())
	th.Equals(t, true, c.IsDeletedChecksum(item.Md5Sum))

	err = c.Add(*item)
	th.Ok(t, err)
	th.Equals(t, 1, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	th.Equals(t, false, c.IsDeletedChecksum(item.Md5Sum))

}

func TestSetMissing(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	c := NewCatalog()
	expectedItem, _ := NewItem(fs, path)
	err := c.Set(*expectedItem)
	th.Ok(t, err)
	th.Equals(t, 1, c.Count())
	th.Equals(t, 0, c.DeletedCount())
	c.DeletePath(path)
	th.Equals(t, 1, c.DeletedCount())
	th.Equals(t, 0, c.Count())

	deleted := c.IsDeletedChecksum("b3cd1cf6179bca32fd5d76473b129117")
	th.Equals(t, true, deleted)

	path2 := "test_data/test2.txt"
	checksum2 := Checksum("89b2b34c7b8d232041f0fcc1d213d7bc")
	_, err = c.Item(path2)
	th.Nok(t, err, "No such file: "+path2)

	items, err := c.ItemsByChecksum(checksum2)
	th.Nok(t, err, fmt.Sprintf("No such file: %v", checksum2))
	th.Equals(t, []Item{}, items)
}

func TestSetExisting(t *testing.T) {
	fs := afero.NewOsFs()
	path := "test_data/test1.txt"
	c := NewCatalog()
	item, _ := NewItem(fs, path)
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
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	c, err := Read(fs, CatalogFileName)
	th.NokPrefix(t, err, "Cannot read catalog: 'coback.catalog'")
	th.Equals(t, c, nil)
}

func TestReadParseError(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data/subfolder"
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	afero.WriteFile(fs, CatalogFileName, []byte("Not a valid json"), 0644)
	c, err := Read(fs, CatalogFileName)
	th.NokPrefix(t, err, "Cannot parse catalog json: 'coback.catalog'")
	th.Equals(t, c, nil)
}

func createTestCatalog(fs afero.Fs) *catalog {
	c := newcatalog()
	item, _ := NewItem(fs, "subfolder/file1.bin")
	c.Add(*item)
	item, _ = NewItem(fs, "test2.txt")
	c.Add(*item)
	item, _ = NewItem(fs, "test1.txt")
	c.Add(*item)
	item, _ = NewItem(fs, "subfolder/file2.bin")
	c.Add(*item)
	return c
}

func createOneLevelTestCatalog(fs afero.Fs) *catalog {
	c := newcatalog()
	item, _ := NewItem(fs, "file1.bin")
	c.Add(*item)
	item, _ = NewItem(fs, "file2.bin")
	c.Add(*item)
	return c
}

func TestWriteReadOneLevel(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data/subfolder"
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	c := createOneLevelTestCatalog(fs)
	c.Write(fs, CatalogFileName)
	c2, err := Read(fs, CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, c, c2)
}

func TestWriteReadRecursive(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	c := createTestCatalog(fs)
	c.Write(fs, CatalogFileName)
	c2, err := Read(fs, CatalogFileName)
	th.Ok(t, err)
	th.Equals(t, c, c2)
}

func TestClone(t *testing.T) {
	basePath, _ := os.Getwd()
	path := "test_data"
	fs := fsh.CreateSafeFs(filepath.Join(basePath, path))
	c := createTestCatalog(fs)
	c.DeleteChecksum("1234")
	clone := c.Clone().(*catalog)
	th.Equals(t, c, clone)
	th.Assert(t, &c != &clone, "clone should be a different object")
	th.Equals(t, c.Items, clone.Items)
	th.Assert(t, &c.Items != &clone.Items, "clone.Items should be a different object")
	th.Equals(t, c.checksumToPaths, clone.checksumToPaths)
	th.Assert(t, &c.checksumToPaths != &clone.checksumToPaths, "clone.pathToIdx should be a different object")
	th.Equals(t, c.DeletedChecksums, clone.DeletedChecksums)
	th.Assert(t, &c.DeletedChecksums != &clone.DeletedChecksums, "clone.checksumToIdx should be a different object")
}

func TestFilterNew(t *testing.T) {
	a := Item{Path: "some/path/to/a", Md5Sum: "a", Size: 42}
	b := Item{Path: "some/other/b", Md5Sum: "b", Size: 213456}
	c := Item{Path: "path_to/c", Md5Sum: "c", Size: 987}
	collection := NewCatalog()
	newFolder := NewCatalog()

	collection.Add(a)
	collection.Add(b)
	expected := NewCatalog()
	th.Equals(t, expected, newFolder.FilterNew(collection))

	newFolder.Add(b)
	th.Equals(t, expected, newFolder.FilterNew(collection))

	newFolder.Add(c)
	expected.Add(c)
	th.Equals(t, expected, newFolder.FilterNew(collection))

	collection.Add(c)
	expected = NewCatalog()
	th.Equals(t, expected, newFolder.FilterNew(collection))
}

func TestFilterNewWithDeleted(t *testing.T) {
	a := Item{Path: "some/path/to/a", Md5Sum: "a", Size: 42}
	b := Item{Path: "some/other/b", Md5Sum: "b", Size: 213456}
	c := Item{Path: "path_to/c", Md5Sum: "c", Size: 987}
	collection := NewCatalog()
	newFolder := NewCatalog()

	collection.Add(a)
	collection.DeleteChecksum(b.Md5Sum)
	expected := NewCatalog()
	th.Equals(t, expected, newFolder.FilterNew(collection))

	newFolder.Add(b)
	th.Equals(t, expected, newFolder.FilterNew(collection))

	newFolder.Add(c)
	expected.Add(c)
	th.Equals(t, expected, newFolder.FilterNew(collection))

	collection.Add(c)
	expected = NewCatalog()
	th.Equals(t, expected, newFolder.FilterNew(collection))
}

func TestAllItems(t *testing.T) {
	a := Item{Path: "some/path/to/a", Md5Sum: "a", Size: 42}
	b := Item{Path: "some/other/b", Md5Sum: "b", Size: 213456}
	c := Item{Path: "path_to/c", Md5Sum: "c", Size: 987}

	paths := func(items <-chan Item) <-chan string {
		ret := make(chan string)
		go func() {
			for item := range items {
				ret <- item.Path
			}
		}()
		return ret
	}

	collection := NewCatalog()
	actual := cth.ReadStringChannel(paths(collection.AllItems()))
	th.Equals(t, []string{}, actual)

	collection.Add(a)
	actual = cth.ReadStringChannel(paths(collection.AllItems()))
	th.Equals(t, []string{a.Path}, actual)

	collection.Add(b)
	actual = cth.ReadStringChannel(paths(collection.AllItems()))
	th.Equals(t, []string{b.Path, a.Path}, actual)

	collection.Add(c)
	actual = cth.ReadStringChannel(paths(collection.AllItems()))
	th.Equals(t, []string{c.Path, b.Path, a.Path}, actual)
}
