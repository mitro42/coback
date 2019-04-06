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
