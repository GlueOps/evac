package cli

import (
	"errors"
	"time"

	"github.com/GlueOps/evac/internal/selection"
)

// parseTime is a forgiving RFC3339 parse; a bad value simply omits the
// snapshot timestamp from the picker title rather than failing the command.
func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// asControlPlaneErr is errors.As with the concrete type, kept here so the
// command files stay readable.
func asControlPlaneErr(err error, target **selection.ControlPlaneInFileError) bool {
	return errors.As(err, target)
}
