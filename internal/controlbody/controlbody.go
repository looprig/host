// Package controlbody strictly reads interrupt and restore request bodies
// before a dispatch attempt. These kinds carry a principal but no message.
package controlbody

import (
	"encoding/json"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/host/internal/bodyclass"
)

// MalformedError means no Host can read the immutable body.
type MalformedError struct{ Cause error }

func (e *MalformedError) Error() string { return "controlbody: malformed request: " + e.Cause.Error() }

// Unwrap returns the decode failure.
func (e *MalformedError) Unwrap() error { return e.Cause }

// UnsupportedError means a newer Host may understand the body.
type UnsupportedError struct{ Cause error }

func (e *UnsupportedError) Error() string {
	return "controlbody: unreadable request: " + e.Cause.Error()
}

// Unwrap returns the decode failure.
func (e *UnsupportedError) Unwrap() error { return e.Cause }

// CheckInterrupt decodes a stored interrupt request. An absent legacy body
// carries no members and is allowed.
func CheckInterrupt(body []byte, session sessionwire.SessionID, command sessionwire.CommandID) (bodyclass.Members, error) {
	var request sessionwire.InterruptRequest
	return checkControlBody(body, &request, func() (sessionwire.SessionID, sessionwire.CommandID, *sessionwire.Principal) {
		return request.SessionID, request.CommandID, request.Principal
	}, session, command)
}

// CheckRestore decodes a stored restore request. An absent legacy body
// carries no members and is allowed.
func CheckRestore(body []byte, session sessionwire.SessionID, command sessionwire.CommandID) (bodyclass.Members, error) {
	var request sessionwire.RestoreRequest
	return checkControlBody(body, &request, func() (sessionwire.SessionID, sessionwire.CommandID, *sessionwire.Principal) {
		return request.SessionID, request.CommandID, request.Principal
	}, session, command)
}

func checkControlBody(body []byte, into any, read func() (sessionwire.SessionID, sessionwire.CommandID, *sessionwire.Principal), session sessionwire.SessionID, command sessionwire.CommandID) (bodyclass.Members, error) {
	if len(body) == 0 {
		return bodyclass.Members{}, nil
	}
	if err := json.Unmarshal(body, into); err != nil {
		if bodyclass.Permanent(err) {
			return bodyclass.Members{}, &MalformedError{Cause: err}
		}
		return bodyclass.Members{}, &UnsupportedError{Cause: err}
	}
	decodedSession, decodedCommand, principal := read()
	if decodedSession != session || decodedCommand != command {
		return bodyclass.Members{}, &MalformedError{Cause: errors.New("the control body names another session or command")}
	}
	return bodyclass.Members{Principal: principal}, nil
}
