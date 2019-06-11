package catalog

import (
	"crypto/md5"
	"encoding/hex"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// Item represents the metadata for one file stored in the catalog
type Item struct {
	Path             string   `json:"path"`
	Size             int64    `json:"size"`
	ModificationTime string   `json:"modification_time"`
	Md5Sum           Checksum `json:"md5sum"`
	Deleted          bool     `json:"deleted"`
}

// NewItem creates an Item for the specified file
func NewItem(fs afero.Fs, path string) (*Item, error) {
	buf, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot open file")
	}

	fi, err := fs.Stat(path)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot get file info")
	}

	hash := md5.New()
	hash.Write(buf)
	return &Item{
		Path:             path,
		Size:             fi.Size(),
		ModificationTime: fi.ModTime().Format(time.RFC3339Nano),
		Md5Sum:           Checksum(hex.EncodeToString(hash.Sum(nil))),
		Deleted:          false,
	}, nil
}
