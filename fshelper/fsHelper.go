package fshelper

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// EnsureDirectoryExist makes sure that the path it gets is a directory.
// Returns an error if the path is a file or if it doesn't exist and cannot be created.
func EnsureDirectoryExist(fs afero.Fs, path string) error {
	if path == "." {
		return nil
	}
	stat, err := fs.Stat(path)
	if err == nil && !stat.IsDir() { // the directory's name is already in use by a file
		return fmt.Errorf("Path is a file '%v'", path)
	} else if err != nil && os.IsNotExist(err) { // the directory doesn't exist
		err = fs.MkdirAll(path, 0755)
		if err != nil {
			return errors.Wrapf(err, "Cannot create directory '%v'", path)
		}
	}

	return nil
}

// copyFileContent copies the content (and only the content) of a file between file systems. The containing directories are automatically
// created as necessary, the path relative to the root of the FS will be the same in the destination FS as it was in the source FS.
//  Metadata of the file is not copied.
func copyFileContent(sourceFs afero.Fs, sourcePath string, destinationFs afero.Fs, destinationPath string) (int64, error) {
	sourceFile, err := sourceFs.Open(sourcePath)
	if err != nil {
		return 0, err
	}
	defer sourceFile.Close()

	err = EnsureDirectoryExist(destinationFs, path.Dir(destinationPath))
	if err != nil {
		return 0, err
	}
	destinationFile, err := destinationFs.Create(destinationPath)
	if err != nil {
		return 0, errors.Wrapf(err, "Cannot create destination file '%v'", destinationPath)
	}
	defer destinationFile.Close()
	return io.Copy(destinationFile, sourceFile)
}

// SetFileAttributes sets the modification and access times of a file in a FS as described in a catalog.Item
func SetFileAttributes(fs afero.Fs, path, timestamp string) error {
	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return errors.Wrapf(err, "Cannot parse modification time of file '%v' ('%v')", path, timestamp)
	}
	fs.Chtimes(path, t, t)
	return nil
}

// CopyFile copies a file described in a catalog.Item between two file systems.
// The access and modification time stamps are set to the time specified in the item struct.
func CopyFile(sourceFs afero.Fs, path, timestamp string, destinationFs afero.Fs) error {
	size, err := copyFileContent(sourceFs, path, destinationFs, path)

	if err != nil {
		return errors.Wrapf(err, "Failed to copy file '%v'", path)
	}

	fiSource, err := sourceFs.Stat(path)
	if err != nil || fiSource.Size() != size {
		return errors.Wrapf(err, "Incorrect file size after copy '%v'", path)
	}

	err = SetFileAttributes(destinationFs, path, timestamp)
	return errors.Wrapf(err, "Failed to set file attributes '%v'", path)
}

// NextUnusedFolder returns a smallest positive integer as a string that can be used as the name of a new folder
// in the root of the FS
func NextUnusedFolder(fs afero.Fs) string {
	nextFolder := 0
	for {
		nextFolder++
		folderName := strconv.Itoa(nextFolder)

		if _, err := fs.Stat(folderName); os.IsNotExist(err) {
			return folderName
		}
	}
}

// CreateSafeFs creates a temporary memory file system layer on top of a real folder in the OS file system.
// The returned fs can be safely modified, its changes won't change to OS file system.
func CreateSafeFs(basePath string) afero.Fs {
	base := afero.NewBasePathFs(afero.NewOsFs(), basePath)
	roBase := afero.NewReadOnlyFs(base)
	sfs := afero.NewCopyOnWriteFs(roBase, afero.NewMemMapFs())
	return sfs
}
