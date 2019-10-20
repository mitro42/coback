package scan

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mitro42/coback/catalog"
	fsh "github.com/mitro42/coback/fshelper"
	"github.com/spf13/afero"
)

type mockProgressBar struct {
	value         int64
	total         int64
	elapsed       time.Duration
	done          bool
	incrByCount   int
	setTotalCount int
	mux           sync.Mutex
}

func newMockProgressBar() *mockProgressBar {
	return &mockProgressBar{}
}

func (m *mockProgressBar) SetTotal(total int64, final bool) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.total = total
	m.done = final
	m.setTotalCount++
}

func (m *mockProgressBar) IncrBy(n int, wdd ...time.Duration) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.value += int64(n)
	m.elapsed += wdd[0]
	m.incrByCount++
}

func (m *mockProgressBar) Current() int64 {
	m.mux.Lock()
	defer m.mux.Unlock()
	return m.value
}

func changeFileContent(fs afero.Fs, path string) error {
	f, err := fs.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte("Some new stuff\n")); err != nil {
		return err
	}
	return f.Close()
}

type dummyFileDescription struct {
	Path    string
	Size    int64
	Md5Sum  catalog.Checksum
	Content string
}

var dummies = []dummyFileDescription{
	{"subfolder/dummy1", 32, "30fac14a21fcc0c2d126a159beb14cb5", "This is just some dummy content\n"},
	{"dummy2", 351, "546ea07b13dc314506dc2e48dcc2a9d1", "Just some other content... On the other hand, we denounce with righteous indignation and dislike men who are so beguiled and demoralized by the charms of pleasure of the moment, so blinded by desire, that they cannot foresee the pain and trouble that are bound to ensue; and equal blame belongs to those who fail in their duty through weakness of will"},
}

func createDummyFile(fs afero.Fs, file dummyFileDescription) error {
	f, err := fs.OpenFile(file.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(file.Content)); err != nil {
		return err
	}
	return f.Close()
}

// createMemFsTestData creates an afero.MemFs and copies to contents of the test_data folder into it.
// This is necessary to work around an afero limitation:
// renaming and removing files from a CopyOnWriteFs is not yet supported.
func createMemFsTestData() afero.Fs {
	basePath, _ := os.Getwd()
	memFs := afero.NewMemMapFs()
	testDataFs := afero.NewBasePathFs(memFs, "test_data")
	diskFs := fsh.CreateSafeFs(filepath.Join(filepath.Dir(basePath), "test_data"))
	afero.Walk(diskFs, ".", func(p string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			return nil
		}
		fsh.CopyFile(diskFs, p, fi.ModTime().Format(time.RFC3339Nano), testDataFs)
		return nil
	})
	return memFs
}
