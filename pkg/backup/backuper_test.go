package backup

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
)

type testBackupSharder struct {
	data shardDetermination
	err  error
}

func (bs *testBackupSharder) determineShards(_ context.Context) (shardDetermination, error) {
	if bs.err != nil {
		return nil, bs.err
	}
	return bs.data, nil
}

func TestPopulateBackupShardField(t *testing.T) {
	errVersion := errors.New("versioner error")
	errVersioner := newTestVersioner(withVersionErr(errVersion))
	goodVersioner := newTestVersioner(withVersion(minVersShardOp))
	oldVersioner := newTestVersioner(withVersion(-1))

	// Create tables to reset field state
	tableData := func() []clickhouse.Table {
		return []clickhouse.Table{
			{
				Database: "a",
				Name:     "present",
			},
			{
				Database: "a",
				Name:     "absent",
			},
			{
				Database: "b",
				Name:     "present",
			},
		}
	}

	errShard := errors.New("backup sharder error")
	errSharder := &testBackupSharder{err: errShard}
	staticSharder := &testBackupSharder{
		data: shardDetermination{
			"a.present": true,
			"a.absent":  false,
			"b.present": true,
		},
	}
	emptySharder := &testBackupSharder{
		data: shardDetermination{},
	}

	testcases := []struct {
		name        string
		shardConfig bool
		v           versioner
		bs          backupSharder
		expect      []clickhouse.Table
		expectErr   error
	}{
		{
			name:        "Test versioner error",
			shardConfig: true,
			v:           errVersioner,
			bs:          staticSharder,
			expectErr:   errVersion,
		},
		{
			name:        "Test incompatible version",
			shardConfig: true,
			v:           oldVersioner,
			bs:          staticSharder,
			expectErr:   errShardOperationVers,
		},
		{
			name:        "Test incompatible version without sharding config",
			shardConfig: false,
			v:           oldVersioner,
			bs:          staticSharder,
			expect: []clickhouse.Table{
				{
					Database:   "a",
					Name:       "present",
					BackupType: clickhouse.ShardBackupFull,
				},
				{
					Database:   "a",
					Name:       "absent",
					BackupType: clickhouse.ShardBackupFull,
				},
				{
					Database:   "b",
					Name:       "present",
					BackupType: clickhouse.ShardBackupFull,
				},
			},
		},
		{
			name:        "Test sharder error",
			shardConfig: true,
			v:           goodVersioner,
			bs:          errSharder,
			expectErr:   errShard,
		},
		{
			name:        "Test incomplete replica data",
			shardConfig: true,
			v:           goodVersioner,
			bs:          emptySharder,
			expectErr:   errUnknownBackupShard,
		},
		{
			name:        "Test normal sharding",
			shardConfig: true,
			v:           goodVersioner,
			bs:          staticSharder,
			expect: []clickhouse.Table{
				{
					Database:   "a",
					Name:       "present",
					BackupType: clickhouse.ShardBackupFull,
				},
				{
					Database:   "a",
					Name:       "absent",
					BackupType: clickhouse.ShardBackupSchema,
				},
				{
					Database:   "b",
					Name:       "present",
					BackupType: clickhouse.ShardBackupFull,
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Log(tc.name)
		cfg := &config.Config{
			General: config.GeneralConfig{
				ShardedOperation: tc.shardConfig,
			},
		}
		b := NewBackuper(cfg,
			WithVersioner(tc.v),
			WithBackupSharder(tc.bs),
		)
		tables := tableData()
		err := b.populateBackupShardField(context.Background(), tables)
		if !errors.Is(err, tc.expectErr) {
			t.Fatalf("expected error %v, got %v", tc.expectErr, err)
		}
		if err != nil {
			continue
		}
		if !reflect.DeepEqual(tables, tc.expect) {
			t.Fatalf("expected %+v, got %+v", tc.expect, tables)
		}
	}
}
