package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/afero"
)

// ensureDirectoryExist makes sure that the path it gets is a directory.
// Returns an error if the path is a file or if it doesn't exist and cannot be created.
func ensureDirectoryExist(fs afero.Fs, path string) error {
	stat, err := fs.Stat(path)

	if os.IsNotExist(err) {
		err = fs.MkdirAll(path, 0755)
		if err != nil {
			return fmt.Errorf("Failed to create directory: '%v'", path)
		}
		return nil
	}

	if stat.Mode().IsRegular() {
		return fmt.Errorf("Path is a file: '%v'", path)
	}

	return nil
}

// returns a smallest positive integer as a string that can be used as the name of a new folder
// in the root of the FS
func nextUnusedFolder(fs afero.Fs) string {
	nextFolder := 0
	for {
		nextFolder++
		folderName := strconv.Itoa(nextFolder)

		if _, err := fs.Stat(folderName); os.IsNotExist(err) {
			return folderName
		}
	}
}
