package backup

import (
	"context"
	"errors"
)

const (
	minVersShardOp = 21000000
)

var (
	errShardOperationVers = errors.New("sharded operations are only supported for " +
		"clickhouse-server >= v21.x")
	errShardOperationUnsupported = errors.New("sharded operations are not supported")
)

// versioner is an interface for determining the version of Clickhouse
type versioner interface {
	GetVersion(context.Context) (int, error)
}

// canShardOperation returns whether or not sharded backup creation is supported
func canShardOperation(ctx context.Context, v versioner) error {
	version, err := v.GetVersion(ctx)
	if err != nil {
		return err
	}
	if version < minVersShardOp {
		return errShardOperationVers
	}
	return nil
}
