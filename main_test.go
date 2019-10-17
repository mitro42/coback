package main

import (
	"testing"

	"github.com/mitro42/coback/catalog"
	th "github.com/mitro42/testhelper"
	"github.com/spf13/afero"
)

func TestSyncImportCreate(t *testing.T) {
	memFs := afero.NewMemMapFs()
	importFs, err := initializeFolder(memFs, "holiday_pictures", "Import")
	th.Ok(t, err)
	c, err := syncCatalogWithImportFolder(importFs)
	th.Ok(t, err)

	th.Equals(t, catalog.NewCatalog(), c)
}
