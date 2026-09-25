// Package gateresponse reads a gate_response command's private body, the one
// rule both of its readers share.
//
// TWO READERS, ONE RULE, AND THE ORDER BETWEEN THEM IS THE POINT. The applier
// reads the body BEFORE it authorizes a dispatch attempt, to refuse a body no
// Host could ever apply while the command is still pending or claimed and a
// rejection is still possible. The runtime adapter reads it AFTER, to build
// harness's decoded answer. If the two applied different rules, a body the
// first accepted and the second refused would fail after the attempt was
// durable — and once an attempt exists nothing settles a command except the
// runtime's own evidence, which a refused dispatch never writes. Sharing the
// function is what makes that ordering safe.
// Both readers use bodyclass.Permanent: a future member or version blocks
// rather than destroying an answer a newer Host may apply.
package gateresponse

import (
	"encoding/json"
	"errors"
	"strconv"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/core/uuid"
	"github.com/looprig/host/internal/bodyclass"
)

// Response is one decoded gate_response body.
type Response struct {
	// Request is the Core record Factory admitted, strictly decoded and
	// validated.
	Request sessionwire.GateResponseRequest

	// GateID is the Core gate identity as harness's gate identity: a non-zero
	// UUID, because harness mints every gate id as one.
	GateID uuid.UUID
}

// MalformedError reports a body no Host can ever apply. It is PERMANENT: the
// body is immutable once admitted, so a retry, or another Host, reads the same
// bytes and reaches the same answer.
type MalformedError struct {
	Reason string
	Cause  error
}

func (e *MalformedError) Error() string {
	message := "gateresponse: the gate response body is malformed: " + e.Reason
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the lower-level failure, if any.
func (e *MalformedError) Unwrap() error { return e.Cause }

// UnsupportedError reports a body this Host cannot read but a newer one may.
type UnsupportedError struct{ Cause error }

func (e *UnsupportedError) Error() string {
	return "gateresponse: this Host cannot read the gate response: " + e.Cause.Error()
}

// Unwrap returns the decode failure.
func (e *UnsupportedError) Unwrap() error { return e.Cause }

// ErrEmptyBody is the cause of a MalformedError for a command with no inline
// body. A body stored behind an object reference is not this error: its bytes
// exist, this Host does not dereference them, and that is the caller's to say.
var ErrEmptyBody = errors.New("gateresponse: the command carries no inline body")

// Decode reads one gate_response body admitted for (session, command).
//
// It is Core's strict decoder and Core's validation — exactly one expected-open
// version, a non-empty action, non-nil values — plus the two things only the
// reader of a STORED body can check: that the body names the session and the
// command it is stored under, and that its gate identity is one harness could
// have minted.
func Decode(body []byte, session sessionwire.SessionID, command sessionwire.CommandID) (Response, error) {
	if len(body) == 0 {
		return Response{}, &MalformedError{Reason: "it is empty", Cause: ErrEmptyBody}
	}
	var request sessionwire.GateResponseRequest
	if err := json.Unmarshal(body, &request); err != nil {
		if !bodyclass.Permanent(err) {
			return Response{}, &UnsupportedError{Cause: err}
		}
		return Response{}, &MalformedError{Reason: "it is not a Core gate-response request", Cause: err}
	}
	if err := request.Validate(); err != nil {
		return Response{}, &MalformedError{Reason: "Core refuses it", Cause: err}
	}
	if request.SessionID != session || request.CommandID != command {
		return Response{}, &MalformedError{
			Reason: "it names (" + strconv.Quote(string(request.SessionID)) + ", " + strconv.Quote(string(request.CommandID)) +
				"), which is not the record it is stored under",
		}
	}
	gateID, err := uuid.Parse(string(request.GateID))
	if err == nil && gateID.IsZero() {
		err = errors.New("the zero UUID names no gate")
	}
	if err != nil {
		return Response{}, &MalformedError{Reason: "its gate identity is not one harness mints (a non-zero UUID)", Cause: err}
	}
	return Response{Request: request, GateID: gateID}, nil
}
