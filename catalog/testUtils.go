package catalog

import (
	"os"
	"sync"
	"time"

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
	Md5Sum  Checksum
	Content string
}

var dummies = []dummyFileDescription{
	{"subfolder/dummy1", 32, "30fac14a21fcc0c2d126a159beb14cb5", "This is just some dummy content\n"},
	{"dummy2", 351, "546ea07b13dc314506dc2e48dcc2a9d1", "Just some other content... On the other hand, we denounce with righteous indignation and dislike men who are so beguiled and demoralized by the charms of pleasure of the moment, so blinded by desire, that they cannot foresee the pain and trouble that are bound to ensue; and equal blame belongs to those who fail in their duty through weakness of will"},
}

func createSafeFs(basePath string) afero.Fs {
	base := afero.NewBasePathFs(afero.NewOsFs(), basePath)
	roBase := afero.NewReadOnlyFs(base)
	sfs := afero.NewCopyOnWriteFs(roBase, afero.NewMemMapFs())
	return sfs
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

// readStringChannel collects the items read from the channel to a string slice and checks
// that the channel contents are terminated with an empty string.
// Returns the contents and panics if the channel wasn't properly terminated.
func readStringChannel(files <-chan string) []string {
	ret := make([]string, 0)
	closingElementFound := false
	for file := range files {
		if file == "" {
			closingElementFound = true
			break
		}
		ret = append(ret, file)
	}
	if !closingElementFound {
		panic("closing element not found")
	}
	return ret
}

// readInt64Channel collects the items read from the channel to a int64 slice and checks
// that the channel contents are terminated with -1
// Returns the contents and panics if the channel wasn't properly terminated.
func readInt64Channel(sizes <-chan int64) []int64 {
	ret := make([]int64, 0)
	closingElementFound := false
	for size := range sizes {
		if size == -1 {
			closingElementFound = true
			break
		}
		ret = append(ret, size)
	}
	if !closingElementFound {
		panic("closing element not found")
	}
	return ret
}
