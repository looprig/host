// Package inputbody strictly reads a stored Core input request before the
// dispatch attempt, without rewriting the bytes handed to a product decoder.
package inputbody

import (
	"encoding/json"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host/internal/bodyclass"
)

// MalformedError means no Host can read the immutable body.
type MalformedError struct{ Cause error }

func (e *MalformedError) Error() string {
	return "inputbody: malformed input request: " + e.Cause.Error()
}

// Unwrap returns the decode failure.
func (e *MalformedError) Unwrap() error { return e.Cause }

// UnsupportedError means a newer Host may understand the body.
type UnsupportedError struct{ Cause error }

func (e *UnsupportedError) Error() string {
	return "inputbody: unreadable input request: " + e.Cause.Error()
}

// Unwrap returns the decode failure.
func (e *UnsupportedError) Unwrap() error { return e.Cause }

// ErrEmptyBody is the cause of a malformed inline input with no bytes.
var ErrEmptyBody = errors.New("inputbody: no inline body")

// Check decodes the immutable body for its durable session and command.
func Check(body []byte, session sessionwire.SessionID, command sessionwire.CommandID) (bodyclass.Members, error) {
	if len(body) == 0 {
		return bodyclass.Members{}, &MalformedError{Cause: ErrEmptyBody}
	}
	var request sessionwire.InputRequest
	if err := json.Unmarshal(body, &request); err != nil {
		if bodyclass.Permanent(err) {
			return bodyclass.Members{}, &MalformedError{Cause: err}
		}
		return bodyclass.Members{}, &UnsupportedError{Cause: err}
	}
	if request.SessionID != session || request.CommandID != command {
		return bodyclass.Members{}, &MalformedError{Cause: errors.New("the input body names another session or command")}
	}
	return bodyclass.Members{Principal: request.Principal, Metadata: request.Metadata}, nil
}
