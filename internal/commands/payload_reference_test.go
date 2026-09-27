package commands

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/sessionstore"
)

type payloadObjectsFake struct {
	body           []byte
	metadata       sessionwire.ObjectMetadata
	metadataCalls  int
	readCalls      int
	metadataErr    error
	streamErr      error
	foreignSession sessionwire.SessionID
}

func (f *payloadObjectsFake) GetObjectMetadata(_ context.Context, req sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error) {
	f.metadataCalls++
	if f.metadataErr != nil {
		return sessionwire.ObjectMetadata{}, f.metadataErr
	}
	if f.foreignSession != "" && req.SessionID != f.foreignSession {
		return sessionwire.ObjectMetadata{}, &sessionstore.ObjectError{Code: sessionstore.ObjectErrorMetadataUnavailable}
	}
	if req.TenantID != testTenant || req.SessionID != testSession || req.ExpectedKind != sessionstore.ObjectKindCommandPayload || req.Reference != f.metadata.Reference {
		return sessionwire.ObjectMetadata{}, &sessionstore.ObjectError{Code: sessionstore.ObjectErrorInvalid}
	}
	return f.metadata, nil
}

func (f *payloadObjectsFake) GetObject(_ context.Context, req sessionstore.GetObjectRequest) (io.ReadCloser, error) {
	f.readCalls++
	if req.TenantID != testTenant || req.SessionID != testSession || req.ExpectedKind != sessionstore.ObjectKindCommandPayload || req.Metadata != f.metadata {
		return nil, &sessionstore.ObjectError{Code: sessionstore.ObjectErrorInvalid}
	}
	if f.streamErr != nil {
		return io.NopCloser(io.MultiReader(bytes.NewReader(f.body[:len(f.body)/2]), failingPayloadStream{f.streamErr})), nil
	}
	return io.NopCloser(bytes.NewReader(f.body)), nil
}

type failingPayloadStream struct{ err error }

func (f failingPayloadStream) Read([]byte) (int, error) { return 0, f.err }

func referenceFixture(f *dispositionFixture) (*payloadObjectsFake, []byte) {
	body := f.store.payloads[commandID(1)].Body
	digest := sha256.Sum256(body)
	objects := &payloadObjectsFake{body: body, metadata: sessionwire.ObjectMetadata{
		Reference: sessionwire.ObjectReference{ObjectID: "v1:command-payload:test:" + hex.EncodeToString(digest[:])},
		SizeBytes: uint64(len(body)), Digest: "sha256:" + hex.EncodeToString(digest[:]),
	}}
	f.store.payloads[commandID(1)] = Payload{Ref: objects.metadata.Reference}
	f.applier.objects = objects
	return objects, body
}

func TestReferencedInputAppliesVerifiedBody(t *testing.T) {
	f := newDispositionFixture(t)
	objects, body := referenceFixture(f)
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied {
		t.Fatalf("Process = (%+v, %v)", outcome, err)
	}
	commands := f.runtime.commands()
	if len(commands) != 1 || !bytes.Equal(commands[0].Payload, body) || commands[0].PayloadRef != (sessionwire.ObjectReference{}) {
		t.Fatalf("runtime commands = %+v, want verified inline bytes and no reference", commands)
	}
	if objects.metadataCalls != 1 || objects.readCalls != 1 {
		t.Fatalf("object reads = %d metadata, %d body", objects.metadataCalls, objects.readCalls)
	}
}

func TestInlineInputDoesNotReadAnObject(t *testing.T) {
	f := newDispositionFixture(t)
	o := &payloadObjectsFake{metadataErr: errors.New("object reader must not be called")}
	f.applier.objects = o
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied || o.metadataCalls != 0 || o.readCalls != 0 {
		t.Fatalf("inline Process = (%+v,%v), object reads %d/%d", outcome, err, o.metadataCalls, o.readCalls)
	}
}

func TestReferencedReaderUsesCommandTenantAndSession(t *testing.T) {
	f := newDispositionFixture(t)
	o, _ := referenceFixture(f)
	outcome, err := f.process()
	if err != nil || outcome.State != StateApplied || o.metadataCalls != 1 || o.readCalls != 1 {
		t.Fatalf("scoped object read = (%+v,%v), metadata=%d body=%d", outcome, err, o.metadataCalls, o.readCalls)
	}
}

func TestReferencedCreateAndGateResponseApplyVerifiedBody(t *testing.T) {
	for _, kind := range []Kind{KindCreate, KindGateResponse} {
		t.Run(string(kind), func(t *testing.T) {
			var f *dispositionFixture
			if kind == KindGateResponse {
				f = gateFixture(t, ownedGate(testEpoch), nil)
			} else {
				f = newDispositionFixture(t, func(f *dispositionFixture) { f.put(kind, StatePending) })
			}
			_, body := referenceFixture(f)
			outcome, err := f.process()
			if err != nil || outcome.State != StateApplied {
				t.Fatalf("Process = (%+v,%v)", outcome, err)
			}
			got := f.runtime.commands()
			if len(got) != 1 || !bytes.Equal(got[0].Payload, body) || got[0].PayloadRef != (sessionwire.ObjectReference{}) {
				t.Fatalf("runtime = %+v", got)
			}
		})
	}
}

func TestInvalidReferencedBodiesAreRejectedBeforeAttempt(t *testing.T) {
	for _, row := range []struct {
		name   string
		change func(*payloadObjectsFake, *dispositionFixture)
	}{
		{"digest mismatch", func(o *payloadObjectsFake, _ *dispositionFixture) {
			o.body = bytes.Replace(o.body, []byte("hello"), []byte("jello"), 1)
		}},
		{"size mismatch", func(o *payloadObjectsFake, _ *dispositionFixture) { o.body = o.body[:len(o.body)-1] }},
		{"oversize", func(o *payloadObjectsFake, f *dispositionFixture) {
			o.metadata.SizeBytes = uint64(f.applier.maxBody + 1)
		}},
		{"missing object", func(o *payloadObjectsFake, _ *dispositionFixture) {
			o.metadataErr = &sessionstore.ObjectError{Code: sessionstore.ObjectErrorMetadataUnavailable}
		}},
		{"wrong kind", func(o *payloadObjectsFake, _ *dispositionFixture) {
			o.metadataErr = &sessionstore.ObjectError{Code: sessionstore.ObjectErrorInvalid}
		}},
		{"cross session", func(o *payloadObjectsFake, _ *dispositionFixture) { o.foreignSession = "session-elsewhere" }},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newDispositionFixture(t)
			o, _ := referenceFixture(f)
			row.change(o, f)
			outcome, err := f.process()
			if err != nil || outcome.State != StateRejected || len(f.runtime.commands()) != 0 || len(f.store.begins) != 0 {
				t.Fatalf("Process = (%+v,%v), runtime=%v", outcome, err, f.runtime.commands())
			}
		})
	}
}

func TestTransientReferencedReadRetriesThenApplies(t *testing.T) {
	f := newDispositionFixture(t)
	o, body := referenceFixture(f)
	o.streamErr = errors.New("temporary backend outage")
	outcome, err := f.process()
	var refusal *ApplyError
	if !errors.As(err, &refusal) || refusal.Refusal != RefusalStore || outcome.State != StateClaimed || len(f.runtime.commands()) != 0 {
		t.Fatalf("first Process = (%+v,%v)", outcome, err)
	}
	o.streamErr = nil
	outcome, err = f.process()
	if err != nil || outcome.State != StateApplied || !bytes.Equal(f.runtime.commands()[0].Payload, body) {
		t.Fatalf("retry = (%+v,%v)", outcome, err)
	}
}

func TestReferencedReadHonorsCancellation(t *testing.T) {
	f := newDispositionFixture(t)
	o, _ := referenceFixture(f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.applier.Process(ctx, command(1, f.stored().record.State))
	if !errors.Is(err, context.Canceled) || len(f.runtime.commands()) != 0 || o.metadataCalls != 0 || o.readCalls != 0 {
		t.Fatalf("canceled Process = %v, runtime=%v, object reads %d/%d", err, f.runtime.commands(), o.metadataCalls, o.readCalls)
	}
}
