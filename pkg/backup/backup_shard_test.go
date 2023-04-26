package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestInShard(t *testing.T) {
	d := shardDetermination{
		"present_db.present_table": true,
		"present_db.absent_table":  false,
		"absent_db.present_table":  false,
		"absent_db.absent_table":   false,
	}
	testcases := []struct {
		name          string
		database      string
		table         string
		expectPresent bool
		expectErr     error
	}{
		{
			name:          "Test nonexistent database",
			database:      "nonexistent",
			table:         "present_table",
			expectPresent: false,
			expectErr:     errUnknownBackupShard,
		},
		{
			name:          "Test nonexistent table",
			database:      "present_db",
			table:         "nonexistent",
			expectPresent: false,
			expectErr:     errUnknownBackupShard,
		},
		{
			name:          "Test in-shard table",
			database:      "present_db",
			table:         "present_table",
			expectPresent: true,
		},
		{
			name:          "Test out-shard table",
			database:      "present_db",
			table:         "absent_table",
			expectPresent: false,
		},
		{
			name:          "Test out-shard database with in-shard table name for other database",
			database:      "absent_db",
			table:         "present_table",
			expectPresent: false,
		},
		{
			name:          "Test out-shard database",
			database:      "absent_db",
			table:         "absent_table",
			expectPresent: false,
		},
	}
	for _, tc := range testcases {
		t.Log(tc.name)
		present, err := d.inShard(tc.database, tc.table)
		if !errors.Is(err, tc.expectErr) {
			t.Fatalf("expected err %q, got %q", tc.expectErr, err)
		}
		if present != tc.expectPresent {
			t.Fatalf("expected in-shard status %v, got %v", tc.expectPresent, present)
		}
	}
}

func TestFullName(t *testing.T) {
	testcases := []struct {
		md     tableReplicaMetadata
		expect string
	}{
		{
			md: tableReplicaMetadata{
				Database: "a",
				Table:    "b",
			},
			expect: "a.b",
		},
		{
			md: tableReplicaMetadata{
				Database: "db",
			},
			expect: "db.",
		},
		{
			md: tableReplicaMetadata{
				Table: "t",
			},
			expect: ".t",
		},
	}
	for _, tc := range testcases {
		if tc.md.fullName() != tc.expect {
			t.Fatalf("expected %q, got %q", tc.expect, tc.md.fullName())
		}
	}
}

func TestFNVHashModShardFunc(t *testing.T) {
	testcases := []struct {
		name      string
		md        *tableReplicaMetadata
		expect    bool
		expectErr error
	}{
		{
			name: "Test empty replica_is_active",
			md: &tableReplicaMetadata{
				Database:        "database",
				Table:           "table",
				ReplicaName:     "replica",
				ReplicaIsActive: map[string]int{},
			},
			expectErr: errNoActiveReplicas,
		},
		{
			name: "Test no active replicas",
			md: &tableReplicaMetadata{
				Database:    "database",
				Table:       "table",
				ReplicaName: "replica",
				ReplicaIsActive: map[string]int{
					"replica": 0,
					"a":       0,
					"b":       0,
				},
			},
			expectErr: errNoActiveReplicas,
		},
		{
			name: "Test single active replica",
			md: &tableReplicaMetadata{
				Database:    "database",
				Table:       "table",
				ReplicaName: "replica",
				ReplicaIsActive: map[string]int{
					"replica": 1,
				},
			},
			expect: true,
		},
		{
			name: "Test single active replica out of many replica_is_active",
			md: &tableReplicaMetadata{
				Database:    "database",
				Table:       "table",
				ReplicaName: "replica",
				ReplicaIsActive: map[string]int{
					"replica": 1,
					"a":       0,
					"b":       0,
				},
			},
			expect: true,
		},
		{
			name: "Test not assigned replica",
			md: &tableReplicaMetadata{
				Database:    "database",
				Table:       "table",
				ReplicaName: "replica",
				ReplicaIsActive: map[string]int{
					"different": 1,
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Log(tc.name)
		got, err := fnvHashModShardFunc(tc.md)
		if !errors.Is(err, tc.expectErr) {
			t.Fatalf("expected error %v, got %v", tc.expectErr, err)
		}
		if got != tc.expect {
			t.Fatalf("expected shard membership %v, got %v", tc.expect, got)
		}
	}
}

type testQuerier struct {
	data      any
	returnErr error
}

func (tq *testQuerier) StructSelectContext(_ context.Context, dest any, _ string) error {
	if tq.returnErr != nil {
		return tq.returnErr
	}
	jsonData, err := json.Marshal(tq.data)
	if err != nil {
		return fmt.Errorf("error encoding data: %w", err)
	}
	return json.NewDecoder(bytes.NewReader(jsonData)).Decode(dest)
}

func TestGetReplicaState(t *testing.T) {
	data := []tableReplicaMetadata{
		{
			Database:        "db",
			Table:           "table",
			ReplicaName:     "replica",
			ReplicaIsActive: map[string]int{},
		},
		{
			Database:    "db2",
			Table:       "table2",
			ReplicaName: "replica2",
			ReplicaIsActive: map[string]int{
				"replica2": 0,
			},
		},
	}
	expectedErr := errors.New("expected error")
	testcases := []struct {
		name      string
		q         querier
		expect    []tableReplicaMetadata
		expectErr error
	}{
		{
			name: "Test error on obtaining replica state",
			q: &testQuerier{
				returnErr: expectedErr,
			},
			expectErr: expectedErr,
		},
		{
			name: "Test pulling data",
			q: &testQuerier{
				data: data,
			},
			expect: data,
		},
	}
	for _, tc := range testcases {
		t.Log(tc.name)
		rd := newReplicaDeterminer(tc.q, nil)
		got, err := rd.getReplicaState(context.Background())
		if !errors.Is(err, tc.expectErr) {
			t.Fatalf("expected error %v, got %v", tc.expectErr, err)
		}
		if err != nil {
			continue
		}
		if !reflect.DeepEqual(got, tc.expect) {
			t.Fatalf("expected data %v, got %v", tc.expect, got)
		}
	}
}

var errNameSharder = errors.New("expected error")

func nameSharder(md *tableReplicaMetadata) (bool, error) {
	if md.Table == "error" {
		return false, errNameSharder
	}
	if md.Table == "present" {
		return true, nil
	}
	return false, nil
}

func TestDetermineShards(t *testing.T) {
	expectErr := errors.New("expected error")
	testcases := []struct {
		name      string
		q         querier
		expect    shardDetermination
		expectErr error
	}{
		{
			name: "Test query error",
			q: &testQuerier{
				returnErr: expectErr,
			},
			expectErr: expectErr,
		},
		{
			name: "Test shard func error",
			q: &testQuerier{
				data: []tableReplicaMetadata{
					{
						Table: "error",
					},
				},
			},
			expectErr: errNameSharder,
		},
		{
			name: "Test normal operation",
			q: &testQuerier{
				data: []tableReplicaMetadata{
					{
						Database: "a",
						Table:    "present",
					},
					{
						Database: "a",
						Table:    "absent",
					},
					{
						Database: "b",
						Table:    "present",
					},
				},
			},
			expect: shardDetermination{
				"a.present": true,
				"a.absent":  false,
				"b.present": true,
			},
		},
	}
	for _, tc := range testcases {
		t.Log(tc.name)
		rd := newReplicaDeterminer(tc.q, nameSharder)
		got, err := rd.determineShards(context.Background())
		if !errors.Is(err, tc.expectErr) {
			t.Fatalf("expected %v, got %v", tc.expectErr, err)
		}
		if err != nil {
			continue
		}
		if !reflect.DeepEqual(got, tc.expect) {
			t.Fatalf("expected data %v, got %v", tc.expect, got)
		}
	}
}
