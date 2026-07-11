package wazero_state_machine

import (
	"context"

	"github.com/logbn/zongzi"
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
