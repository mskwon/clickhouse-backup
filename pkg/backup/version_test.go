package backup

import (
	"context"
	"errors"
	"testing"
)

type testVersionerOpt func(sd *testVersioner)

type testVersioner struct {
	version    int
	versionErr error
}

func newTestVersioner(opts ...testVersionerOpt) *testVersioner {
	v := &testVersioner{}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

func withVersion(version int) testVersionerOpt {
	return func(v *testVersioner) {
		v.version = version
	}
}

func withVersionErr(err error) testVersionerOpt {
	return func(v *testVersioner) {
		v.versionErr = err
	}
}

func (v *testVersioner) GetVersion(_ context.Context) (int, error) {
	if v.versionErr != nil {
		return -1, v.versionErr
	}
	return v.version, nil
}

func TestCanShardOperation(t *testing.T) {
	ctx := context.Background()

	t.Log("test error on version retrieval")
	v := newTestVersioner(withVersionErr(errors.New("error")))
	if err := canShardOperation(ctx, v); err == nil {
		t.Fatal("expected error when getting shard determiner error on version retrieval")
	}

	t.Log("test version too low")
	v = newTestVersioner(withVersion(-1))
	err := canShardOperation(ctx, v)
	if err == nil {
		t.Fatal("expected error when version number is too low")
	}
	if !errors.Is(err, errShardOperationVers) {
		t.Fatalf("expected errShardOperationUnsupported, got %v", err)
	}

	t.Log("test version should be OK")
	v = newTestVersioner(withVersion(minVersShardOp))
	if err = canShardOperation(ctx, v); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
