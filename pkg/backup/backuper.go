package backup

import (
	"context"
	"fmt"
	"path"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/resumable"
	"github.com/AlexAkulov/clickhouse-backup/pkg/storage"
	apexLog "github.com/apex/log"
)

type BackuperOpt func(*Backuper)

type Backuper struct {
	cfg                    *config.Config
	ch                     *clickhouse.ClickHouse
	vers                   versioner
	bs                     backupSharder
	dst                    *storage.BackupDestination
	log                    *apexLog.Entry
	Version                string
	DiskToPathMap          map[string]string
	DefaultDataPath        string
	EmbeddedBackupDataPath string
	isEmbedded             bool
	resume                 bool
	resumableState         *resumable.State
}

func NewBackuper(cfg *config.Config, opts ...BackuperOpt) *Backuper {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
		Log:    apexLog.WithField("logger", "clickhouse"),
	}
	b := &Backuper{
		cfg:  cfg,
		ch:   ch,
		vers: ch,
		bs:   newReplicaDeterminer(ch, fnvHashModShardFunc),
		log:  apexLog.WithField("logger", "backuper"),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func WithVersioner(v versioner) BackuperOpt {
	return func(b *Backuper) {
		b.vers = v
	}
}

func WithBackupSharder(s backupSharder) BackuperOpt {
	return func(b *Backuper) {
		b.bs = s
	}
}

func (b *Backuper) init(ctx context.Context, disks []clickhouse.Disk, backupName string) error {
	var err error
	if disks == nil {
		disks, err = b.ch.GetDisks(ctx)
		if err != nil {
			return err
		}
	}
	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		if b.cfg.ClickHouse.UseEmbeddedBackupRestore && (disk.IsBackup || disk.Name == b.cfg.ClickHouse.EmbeddedBackupDisk) {
			b.EmbeddedBackupDataPath = disk.Path
		}
	}
	b.DiskToPathMap = diskMap
	if b.cfg.General.RemoteStorage != "none" && b.cfg.General.RemoteStorage != "custom" {
		b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, true, backupName)
		if err != nil {
			return err
		}
		if err := b.dst.Connect(ctx); err != nil {
			return fmt.Errorf("can't connect to %s: %v", b.dst.Kind(), err)
		}
	}
	return nil
}

func (b *Backuper) getLocalBackupDataPathForTable(backupName string, disk string, dbAndTablePath string) string {
	backupPath := path.Join(b.DiskToPathMap[disk], "backup", backupName, "shadow", dbAndTablePath, disk)
	if b.isEmbedded {
		backupPath = path.Join(b.DiskToPathMap[disk], backupName, "data", dbAndTablePath)
	}
	return backupPath
}

// populateBackupShardField populates the BackupShard field for a slice of Table structs
func (b *Backuper) populateBackupShardField(ctx context.Context, tables []clickhouse.Table) error {
	// By default, have all fields populated to full backup
	for i := range tables {
		tables[i].BackupType = clickhouse.ShardBackupFull
	}
	if !b.cfg.General.ShardedOperation {
		return nil
	}
	if err := canShardOperation(ctx, b.vers); err != nil {
		return err
	}
	assignment, err := b.bs.determineShards(ctx)
	if err != nil {
		return err
	}
	for i, t := range tables {
		typ, err := assignment.inShard(t.Database, t.Name)
		if err != nil {
			return err
		}
		if !typ {
			tables[i].BackupType = clickhouse.ShardBackupSchema
		}
	}
	return nil
}
