package scan

import "strings"

// FileFilter is an interface for filters used in the catalog scanning pipeline
type FileFilter interface {
	Include(path string) bool
}

// ExtensionFilter filters the files based on their extension. The listed extensions
// will be excluded
func ExtensionFilter(extensions ...string) FileFilter {
	return extensionFilter{
		extensions: extensions,
	}
}

type noFilter struct{}

func (e noFilter) Include(path string) bool {
	return true
}

type extensionFilter struct {
	extensions []string
}

func (e extensionFilter) Include(path string) bool {
	for _, ext := range e.extensions {
		if strings.HasSuffix(path, "."+ext) {
			return false
		}
	}
	return true
}
