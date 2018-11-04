package catalog

import (
	"crypto/md5"
	"encoding/hex"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type catalogItem struct {
	Path             string    `json:"path"`
	Size             int64     `json:"size"`
	ModificationTime time.Time `json:"modification_time"`
	Md5Sum           string    `json:"md5sum"`
	Deleted          bool      `json:"deleted"`
}

// newCatalogItem creates a catalogItem for the specified file
func newCatalogItem(fs afero.Fs, path string) (*catalogItem, error) {
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
	return &catalogItem{
		Path:             path,
		Size:             fi.Size(),
		ModificationTime: fi.ModTime(),
		Md5Sum:           hex.EncodeToString(hash.Sum(nil)),
		Deleted:          false,
	}, nil
}
