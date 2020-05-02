package fshelper

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
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

// SetFileAttributes sets the modification and access times of a file in a file system
func SetFileAttributes(fs afero.Fs, path, timestamp string) error {
	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return errors.Wrapf(err, "Cannot parse modification time of file '%v' ('%v')", path, timestamp)
	}
	fs.Chtimes(path, t, t)
	return nil
}

// CopyFile copies a file between two file systems.
// The destination path of the copy is the same as the source (relative to their respective file systems)
// The access and modification time stamps are set to the timestamp specified in RFC3339Nano format.
func CopyFile(sourceFs afero.Fs, sourcePath string, timestamp string, destinationFs afero.Fs) error {
	return CopyFileTo(sourceFs, sourcePath, timestamp, destinationFs, sourcePath)
}

// CopyFileToFolder copies a file between two file systems to a specified folder that
// can be different from the source file's path.
// The access and modification time stamps are set to the timestamp specified in RFC3339Nano format.
func CopyFileToFolder(sourceFs afero.Fs, sourcePath string, timestamp string, destinationFs afero.Fs, destinationFolderPath string) error {
	_, sourceFileName := path.Split(sourcePath)
	destinationFilePath := path.Join(destinationFolderPath, sourceFileName)
	return CopyFileTo(sourceFs, sourcePath, timestamp, destinationFs, destinationFilePath)
}

// CopyFileTo copies a file between two file systems to a specified path that contains the file name too.
// The destination path be different from the source file's path
// The access and modification time stamps are set to the time specified in the item struct.
func CopyFileTo(sourceFs afero.Fs, sourcePath string, timestamp string, destinationFs afero.Fs, destinationPath string) error {
	size, err := copyFileContent(sourceFs, sourcePath, destinationFs, destinationPath)

	if err != nil {
		return errors.Wrapf(err, "Failed to copy file '%v'", sourcePath)
	}

	fiSource, err := sourceFs.Stat(sourcePath)
	if err != nil || fiSource.Size() != size {
		return errors.Wrapf(err, "Incorrect file size after copy '%v'", sourcePath)
	}

	err = SetFileAttributes(destinationFs, destinationPath, timestamp)
	return errors.Wrapf(err, "Failed to set file attributes '%v'", destinationPath)
}

// NextUnusedFolder returns a smallest positive integer as a string that can be used as the prefix of the
// name of a new folder in the root of the FS
func NextUnusedFolder(fs afero.Fs) string {
	prefixes := make(map[string]bool)
	listing, _ := afero.ReadDir(fs, ".")
	for _, fi := range listing {
		count := strings.SplitN(fi.Name(), "_", 2)[0]
		prefixes[count] = true
	}

	nextFolder := 0
	for {
		nextFolder++
		folderName := strconv.Itoa(nextFolder)

		if _, found := prefixes[folderName]; !found {
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
