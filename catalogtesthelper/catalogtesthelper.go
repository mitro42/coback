package catalogtesthelper

// ReadStringChannel collects the items read from the channel to a string slice and checks
// that the channel contents are terminated with an empty string.
// Returns the contents and panics if the channel wasn't properly terminated.
func ReadStringChannel(files <-chan string) []string {
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

// ReadInt64Channel collects the items read from the channel to a int64 slice and checks
// that the channel contents are terminated with -1
// Returns the contents and panics if the channel wasn't properly terminated.
func ReadInt64Channel(sizes <-chan int64) []int64 {
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
