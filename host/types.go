package wazero_state_machine

import (
	"context"
	"io"

	"github.com/logbn/zongzi"
	"github.com/pantopic/wazero-pool"
)

const (
	flagPersistent = iota
	flagStreaming
)

type (
	Entry  = zongzi.Entry
	Logger = zongzi.Logger
	Result = zongzi.Result
	Shard  = zongzi.Shard

	SnapshotFile           = zongzi.SnapshotFile
	SnapshotFileCollection = zongzi.SnapshotFileCollection

	ShardOption = zongzi.ShardOption
)

type ContextCopy = func(dst, src context.Context) context.Context
type PoolProvider func(shardID uint64) wazeropool.Instance
type ContextInit func(ctx context.Context, shardID, replicaID uint64) context.Context

type StorageExtension interface {
	PrepareSnapshot(ctx context.Context) (cursor any, err error)
	SaveSnapshot(ctx context.Context, cursor any, w io.Writer, _ SnapshotFileCollection, close <-chan struct{}) (err error)
	RecoverFromSnapshot(ctx context.Context, r io.Reader, _ []SnapshotFile, _ <-chan struct{}) (err error)
	Close(ctx context.Context) (err error)
}

type StorageExtensionPersistent interface {
	PrepareSnapshot(ctx context.Context) (cursor any, err error)
	SaveSnapshot(ctx context.Context, cursor any, w io.Writer, close <-chan struct{}) (err error)
	RecoverFromSnapshot(ctx context.Context, r io.Reader, _ <-chan struct{}) (err error)
	Sync(ctx context.Context) (err error)
	Close(ctx context.Context) (err error)
}
