package catalogtesthelper

import (
	"testing"

	th "github.com/mitro42/testhelper"
)

/////////////////////////////////////////////////////////////////////////////////////////
////// String
func channelStringSlice(data []string) chan string {
	ret := make(chan string)
	go func() {
		defer close(ret)
		for _, d := range data {
			ret <- d
		}
	}()
	return ret
}

func TestEmptyOkStringChannel(t *testing.T) {
	data := []string{""}
	c := channelStringSlice(data)
	th.Equals(t, data[0:len(data)-1], ReadStringChannel(c))
}

func TestEmptyNOkStringChannel(t *testing.T) {
	c := make(chan string)
	close(c)
	defer th.ExpectPanic(t, "closing element not found")
	ReadStringChannel(c)
}

func TestOkStringChannel(t *testing.T) {
	data := []string{"aasdf", "42", "zzz", ""}
	c := channelStringSlice(data)
	th.Equals(t, data[0:len(data)-1], ReadStringChannel(c))
}

func TestNOkStringChannel(t *testing.T) {
	data := []string{"aasdf", "42", "zzz"}
	c := channelStringSlice(data)
	defer th.ExpectPanic(t, "closing element not found")
	ReadStringChannel(c)
}

/////////////////////////////////////////////////////////////////////////////////////////
////// Int64
func channelInt64Slice(data []int64) chan int64 {
	ret := make(chan int64)
	go func() {
		defer close(ret)
		for _, d := range data {
			ret <- d
		}
	}()
	return ret
}

func TestEmptyOkInt64Channel(t *testing.T) {
	data := []int64{-1}
	c := channelInt64Slice(data)
	th.Equals(t, data[0:len(data)-1], ReadInt64Channel(c))
}

func TestEmptyNOkInt64Channel(t *testing.T) {
	c := make(chan int64)
	close(c)
	defer th.ExpectPanic(t, "closing element not found")
	ReadInt64Channel(c)
}

func TestOkInt64Channel(t *testing.T) {
	data := []int64{213456, 12, 42, -1}
	c := channelInt64Slice(data)
	th.Equals(t, data[0:len(data)-1], ReadInt64Channel(c))
}

func TestNOkInt64Channel(t *testing.T) {
	data := []int64{0, 123, 42}
	c := channelInt64Slice(data)
	defer th.ExpectPanic(t, "closing element not found")
	ReadInt64Channel(c)
}
