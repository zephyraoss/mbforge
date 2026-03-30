package db

import (
	"context"
	"testing"

	"github.com/zephyraoss/mbforge/internal/libsqlutil"
)

func TestApplyBuildPragmasWorksWithLibSQL(t *testing.T) {
	ctx := context.Background()

	db, err := libsqlutil.OpenLocal(":memory:")
	if err != nil {
		t.Fatalf("OpenLocal: %v", err)
	}
	defer db.Close()

	if err := ApplyBuildPragmas(ctx, db); err != nil {
		t.Fatalf("ApplyBuildPragmas: %v", err)
	}
}
