package cli

import (
	"errors"

	"github.com/GlueOps/evac/internal/selection"
)

// asControlPlaneErr is errors.As with the concrete type, kept here so the
// command files stay readable.
func asControlPlaneErr(err error, target **selection.ControlPlaneInFileError) bool {
	return errors.As(err, target)
}
