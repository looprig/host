// Package createbody reads a create command's private body, which is Core's
// CreateRequest, and re-presents the session's first message in the shape a
// Host's block decoder reads.
//
// IT IS A PACKAGE FOR internal/gateresponse's REASON, not for tidiness. Host
// carries a command's body opaque on purpose — department.RuntimeCommand says
// so in as many words, because "Host is not the semantic validator of a command
// body — Harness is" — so every place that DOES read one is a deliberate
// exception with a rule of its own, and each such rule lives in a package that
// can be read and tested without the runtime edge around it. The gate response
// has two readers; this one has two: the applier checks the Core envelope before
// an attempt, and the harness adapter presents its blocks to the decoder.
//
// IT DECODES NOTHING ONTO A WIRE. The records here are Core's, read from a
// durable body the store already holds; no frame crosses HostLink from this
// package.
package createbody

import (
	"encoding/json"
	"errors"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// MalformedError reports a create body no Host can read as Core's create
// request. It is PERMANENT: the body is immutable once admitted, so a retry, or
// another Host, reads the same bytes and reaches the same answer.
type MalformedError struct{ Cause error }

func (e *MalformedError) Error() string {
	message := "createbody: the create command's body is not a Core create request"
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

// Unwrap returns the decode failure.
func (e *MalformedError) Unwrap() error { return e.Cause }

// UnsupportedError reports bytes this Host cannot interpret but a newer Host
// may. The caller must leave the command claimed without an attempt, so a
// capable successor or Factory's deadline sweep can decide it later.
type UnsupportedError struct{ Cause error }

func (e *UnsupportedError) Error() string {
	return "createbody: this Host cannot read the create request: " + e.Cause.Error()
}
func (e *UnsupportedError) Unwrap() error { return e.Cause }

// ErrEmptyBody is the cause of a MalformedError for a create with no inline
// body. A body stored behind an object reference is not this error: its bytes
// exist, this Host does not dereference them, and that is the caller's to say.
var ErrEmptyBody = errors.New("createbody: the command carries no inline body")

// FirstMessage reads one create's stored body and returns its first message in
// the INPUT-SHAPED body a block decoder reads, or nil when the create carries
// none.
//
// ONE ENCODING, NOT TWO, AND THAT IS THE WHOLE POINT OF THE RE-PRESENTATION. A
// composition names the encoding of a command's private content exactly once
// (harnessadapter's finding H7), and a create's first message is the same
// content an input carries — Core spells both as a JSON block array, and
// CreateRequest.Blocks and InputRequest.Blocks are the same raw member. The
// blocks are therefore returned inside the record an input would have arrived
// in, so a decoder sees exactly the bytes it would have seen had the same
// message been sent as an input a moment later. A second decoder option for a
// create's shape would let one composition decode every input and silently drop
// every create's first words, which is the failure this package exists to make
// impossible rather than to relocate.
//
// A BARE CREATE IS NOT AN ERROR. Core's CreateRequest.Blocks is omitempty and
// its own doc says "Blocks are optional for an idle create"; refusing one would
// make every idle session's first command unsettleable, which is the wedge
// harness v0.36.0 exists to end, reinstated for a subset of sessions.
func FirstMessage(body []byte) ([]byte, error) {
	request, err := decodeCreate(body)
	if err != nil {
		return nil, err
	}
	if len(request.Blocks) == 0 {
		return nil, nil
	}
	// THE ENVELOPE IS THE CREATE'S OWN, CARRIED VERBATIM. The wire version and
	// the command id are what a strict Core decoder checks first, and an
	// invented one would be this package asserting something about a record it
	// only read.
	presented, err := json.Marshal(sessionwire.InputRequest{
		CommandEnvelope: request.CommandEnvelope,
		SessionID:       request.SessionID,
		Blocks:          request.Blocks,
	})
	if err != nil {
		return nil, &MalformedError{Cause: err}
	}
	return presented, nil
}

// Check validates the immutable create body against the durable command's
// identities before an attempt can be written.
func Check(body []byte, session sessionwire.SessionID, command sessionwire.CommandID) error {
	request, err := decodeCreate(body)
	if err != nil {
		return err
	}
	if request.SessionID != session || request.CommandID != command {
		return &MalformedError{Cause: errors.New("the create body names another session or command")}
	}
	return nil
}

func decodeCreate(body []byte) (sessionwire.CreateRequest, error) {
	if len(body) == 0 {
		return sessionwire.CreateRequest{}, &MalformedError{Cause: ErrEmptyBody}
	}
	var request sessionwire.CreateRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return sessionwire.CreateRequest{}, classify(err)
	}
	return request, nil
}

func classify(err error) error {
	var validation *sessionwire.RequestValidationError
	if errors.As(err, &validation) {
		switch validation.Code {
		case sessionwire.RequestValidationCodeUnknownField, sessionwire.RequestValidationCodeUnsupportedVersion:
			return &UnsupportedError{Cause: err}
		case sessionwire.RequestValidationCodeInvalidJSON, sessionwire.RequestValidationCodeDuplicateField,
			sessionwire.RequestValidationCodeMissingField, sessionwire.RequestValidationCodeInvalidField:
			return &MalformedError{Cause: err}
		default:
			return &UnsupportedError{Cause: err}
		}
	}
	var syntax *json.SyntaxError
	var typeError *json.UnmarshalTypeError
	if errors.As(err, &syntax) || errors.As(err, &typeError) {
		return &MalformedError{Cause: err}
	}
	return &UnsupportedError{Cause: err}
}
