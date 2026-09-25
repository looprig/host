// Package bodyclass holds the shared pre-attempt decision about an unreadable
// stored command body. Permanent failures can be rejected; a body a newer Host
// may understand stays claimed and unattempted.
package bodyclass

import (
	"encoding/json"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// Members are the optional decoded command members carried to the runtime.
type Members struct {
	Principal *sessionwire.Principal
	Metadata  sessionwire.MessageMetadata
}

// Permanent reports whether a Core decode failure is malformed under every
// known wire version. Unknown fields and versions are left for newer Hosts.
func Permanent(err error) bool {
	var validation *sessionwire.RequestValidationError
	if errors.As(err, &validation) {
		switch validation.Code {
		case sessionwire.RequestValidationCodeInvalidJSON, sessionwire.RequestValidationCodeDuplicateField,
			sessionwire.RequestValidationCodeMissingField, sessionwire.RequestValidationCodeInvalidField:
			return true
		default:
			return false
		}
	}
	var syntax *json.SyntaxError
	var typeError *json.UnmarshalTypeError
	return errors.As(err, &syntax) || errors.As(err, &typeError)
}
