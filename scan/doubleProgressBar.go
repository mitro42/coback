package scan

// ProgressBar is the minimal progress bar interface used in CoBack.
import (
	"math"
	"time"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

// DoubleProgressBar contains two progress bars, one for file count and one for size
type DoubleProgressBar interface {
	SetTotal(count int64, size int64)
	IncrBy(n int)
	CurrentCount() int64
	CurrentSize() int64
	Wait()
}

type doubleProgressBar struct {
	master   *mpb.Progress
	count    *mpb.Bar
	size     *mpb.Bar
	totalSet bool
}

func (dpb *doubleProgressBar) SetTotal(count int64, size int64) {
	// if dpb.totalSet {
	// 	panic("Total was already set")
	// }
	dpb.count.SetTotal(count, true)
	dpb.size.SetTotal(size, true)
	dpb.totalSet = true
}

func (dpb *doubleProgressBar) IncrBy(n int) {
	dpb.count.IncrBy(1)
	dpb.size.IncrBy(n)
}

func (dpb *doubleProgressBar) CurrentCount() int64 {
	return dpb.count.Current()
}

func (dpb *doubleProgressBar) CurrentSize() int64 {
	return dpb.size.Current()
}

func (dpb *doubleProgressBar) Wait() {
	dpb.master.Wait()
}

func newDoubleProgressBar() DoubleProgressBar {
	p := mpb.New(
		mpb.WithRefreshRate(100 * time.Millisecond),
	)
	countName := "Number of Files"
	countBar := p.AddBar(math.MaxInt64,
		mpb.PrependDecorators(
			decor.Name(countName, decor.WC{W: len(countName) + 2, C: decor.DidentRight}),
			// The counters must be removed on completion because if the total stays 0 (no file found),
			// on completion it looks like it jumps to some memory garbage
			decor.OnComplete(decor.CountersNoUnit("%8d / %8d "), ""),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)
	sizeName := "Processed Size"
	sizeBar := p.AddBar(math.MaxInt64,
		mpb.PrependDecorators(
			decor.Name(sizeName, decor.WC{W: len(countName) + 2, C: decor.DidentRight}),
			// see above
			decor.OnComplete(decor.CountersKibiByte("%8.1f / %8.1f "), ""),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.AverageSpeed(decor.UnitKiB, " %6.1f"),
		),
	)
	return &doubleProgressBar{p, countBar, sizeBar, false}
}
