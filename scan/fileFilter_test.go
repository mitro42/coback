package scan

import (
	"testing"

	th "github.com/mitro42/testhelper"
)

func TestNoFilter(t *testing.T) {
	f := noFilter{}
	th.Equals(t, true, f.Include(""))
	th.Equals(t, true, f.Include("a"))
	th.Equals(t, true, f.Include("a.jpg"))
	th.Equals(t, true, f.Include("a.txt"))
	th.Equals(t, true, f.Include("folder/another/file.txt"))
	th.Equals(t, true, f.Include("folder/another/"))
}

func TestExtensionFilter(t *testing.T) {
	f := ExtensionFilter("txt")
	th.Equals(t, true, f.Include(""))
	th.Equals(t, true, f.Include("a"))
	th.Equals(t, true, f.Include("a.jpg"))
	th.Equals(t, false, f.Include("a.txt"))
	th.Equals(t, false, f.Include("folder/another/file.txt"))
	th.Equals(t, true, f.Include("folder/another/"))
}

func TestExtensionFilterMulti(t *testing.T) {
	f := ExtensionFilter("txt", "jpg")
	th.Equals(t, true, f.Include(""))
	th.Equals(t, true, f.Include("a"))
	th.Equals(t, false, f.Include("a.jpg"))
	th.Equals(t, false, f.Include("a.txt"))
	th.Equals(t, false, f.Include("folder/another/file.txt"))
	th.Equals(t, true, f.Include("include/this/txt/file.png"))
	th.Equals(t, true, f.Include("include/this/too/filetxt"))
	th.Equals(t, true, f.Include("include/this/too/filejpg"))
	th.Equals(t, true, f.Include("include/this/too/fileJP"))
	th.Equals(t, true, f.Include("include/this/too/file.JPG"))
	th.Equals(t, true, f.Include("include/this/too/file.TXT"))
	th.Equals(t, true, f.Include("include/this/too/file.TxT"))
	th.Equals(t, true, f.Include("folder/another/"))
}
