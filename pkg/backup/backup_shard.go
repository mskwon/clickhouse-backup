package backup

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
)

var (
	// errUnknownBackupShard is returned when sharding assignment is requested for a table for which
	// active replication state is not known.
	errUnknownBackupShard = errors.New("unknown backup shard")

	// errNoActiveReplicas is returned when a table is has no current active replicas
	errNoActiveReplicas = errors.New("no active replicas")
)

// shardDetermination is an object holding information on whether or not a table is within the
// backup shard
type shardDetermination map[string]bool

// inShard returns whether or not a given table is within a backup shard
func (d shardDetermination) inShard(database, table string) (bool, error) {
	fullName := fmt.Sprintf("%s.%s", database, table)
	presentInShard, ok := d[fullName]
	if !ok {
		return false, fmt.Errorf("error determining backup shard state for %q: %w", fullName,
			errUnknownBackupShard)
	}
	return presentInShard, nil
}

// backupSharder is an interface which can obtain a shard determination at a given point in time
type backupSharder interface {
	determineShards(ctx context.Context) (shardDetermination, error)
}

// tableReplicaMetadata is data derived from `system.replicas`
type tableReplicaMetadata struct {
	Database        string         `db:"database" json:"database"`
	Table           string         `db:"table" json:"table"`
	ReplicaName     string         `db:"replica_name" json:"replica_name"`
	ReplicaIsActive map[string]int `db:"replica_is_active" json:"replica_is_active"`
}

// fullName returns the table name in the form of `database.table`
func (md *tableReplicaMetadata) fullName() string {
	return fmt.Sprintf("%s.%s", md.Database, md.Table)
}

// querier is an interface that can query Clickhouse
type querier interface {
	StructSelectContext(ctx context.Context, dest any, query string) error
}

// shardFunc is a function that is used to determine whether or not a given database/table should be
// handled by a given replica
type shardFunc func(md *tableReplicaMetadata) (bool, error)

// fnvHashModShardFunc performs a FNV hash of a table name in the form of `database.table` and then
// performs a mod N operation (where N is the number of active replicas) in order to find an index
// in an alphabetically sorted list of active replicas which corresponds to the replica that will
// handle the backup of the table
func fnvHashModShardFunc(md *tableReplicaMetadata) (bool, error) {
	activeReplicas := []string{}
	for replica, active := range md.ReplicaIsActive {
		if active > 0 {
			activeReplicas = append(activeReplicas, replica)
		}
	}
	if len(activeReplicas) == 0 {
		return false, fmt.Errorf("could not determine in-shard state for %s: %w", md.fullName(),
			errNoActiveReplicas)
	}
	sort.Strings(activeReplicas)

	h := fnv.New32a()
	h.Write([]byte(md.fullName()))
	i := h.Sum32() % uint32(len(activeReplicas))
	return activeReplicas[i] == md.ReplicaName, nil
}

// replicaDeterminer is a concrete struct that will query clickhouse to obtain a shard determination
// by examining replica information
type replicaDeterminer struct {
	q  querier
	sf shardFunc
}

// newReplicaDeterminer returns a new shardDeterminer
func newReplicaDeterminer(q querier, sf shardFunc) *replicaDeterminer {
	sd := &replicaDeterminer{
		q:  q,
		sf: sf,
	}
	return sd
}

// getReplicaState obtains the local replication state through a query to `system.replicas`
func (rd *replicaDeterminer) getReplicaState(ctx context.Context) ([]tableReplicaMetadata, error) {
	md := []tableReplicaMetadata{}
	query := "SELECT database, table, replica_name, replica_is_active FROM system.replicas;"
	if err := rd.q.StructSelectContext(ctx, &md, query); err != nil {
		return nil, fmt.Errorf("could not determine replication state: %w", err)
	}
	return md, nil
}

func (rd *replicaDeterminer) determineShards(ctx context.Context) (shardDetermination, error) {
	md, err := rd.getReplicaState(ctx)
	if err != nil {
		return nil, err
	}
	sd := shardDetermination{}
	for _, entry := range md {
		assigned, err := rd.sf(&entry)
		if err != nil {
			return nil, err
		}
		sd[entry.fullName()] = assigned
	}
	return sd, nil
}
