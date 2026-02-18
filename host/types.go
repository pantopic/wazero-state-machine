package wazero_state_machine

import (
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
