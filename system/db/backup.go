package db

import (
	"context"
	"net/http"

	"github.com/ponzu-cms/ponzu/system/db/repo"
)

// Backup writes a snapshot of the system.db database to an HTTP response. The
// output is discarded if we get a cancellation signal.
func Backup(ctx context.Context, res http.ResponseWriter) error {
	err := repo.Backup(ctx, res)

	if err != nil {
		return err
	}

	return nil
}
