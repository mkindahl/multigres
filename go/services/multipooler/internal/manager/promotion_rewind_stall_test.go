// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// TestRewindReadyNeverSetWhenCheckpointStalls reproduces the pooler-internal ROOT
// of MUL-1004 using a mock postgres (the manager's InternalQueryService) to
// imitate a stalled post-promotion checkpoint — no real postgres, no I/O
// throttling.
//
// In the incident (EKS build c675c93e), the strict-AZ failover promoted a
// standby cleanly: it became the rule-named leader (role == Leader, not
// resigned) on a new timeline with its LSN frozen at 0/80000A0. But under the
// EBS IOPS cap the deferred post-promotion checkpoint never completed on the new
// timeline, so pg_control_checkpoint().timeline_id kept lagging the running WAL
// timeline. rewindSourceReady() (pg_replication.go) therefore kept returning
// false, shouldMarkRewindReady (postgres_monitor.go) never fired, and the leader
// never advertised rewind_ready — so the returning stale primary could never be
// pg_rewound/demoted and the shard stayed with two write-capable primaries for
// the full 900s window.
//
// This differs from the timeout/abandon reproduction (the e2e
// TestCascadeReelectionLeavesDualPrimary and
// TestLeaderNeverBecomesRewindReadyAfterFailover), which trips the gate at its
// role != Leader branch after abandoning an in-flight promote. Here the node IS
// the legitimate leader, and the ONLY thing blocking rewind_ready is the stalled
// checkpoint — matching the operative leader p-c3032d9b in the logs.
//
// A real disk always finishes the checkpoint, so this branch of the gate is not
// reachable in the e2e; mocking the query is the faithful way to hold
// rewindSourceReady false. The e2e covers the multi-node consequence (stuck dual
// primary); this covers the manager-level cause.
//
// Test-only: no production logic is changed. The stall is injected purely as the
// boolean the DB would compute for the rewind-source-readiness query.
func TestRewindReadyNeverSetWhenCheckpointStalls(t *testing.T) {
	// rewindSourceReady() runs a single SQL whose result is
	//   pg_control_checkpoint().timeline_id == <running WAL timeline>
	// We match it by the pg_control_checkpoint substring and return the boolean
	// the DB would compute: "f" == checkpoint timeline still lags (stall), "t" ==
	// a checkpoint has completed on the current timeline (recovered).
	const checkpointQueryPattern = "pg_control_checkpoint"

	tests := []struct {
		name             string
		rewindReadyRow   string // value the mock DB returns for the readiness query
		wantRewindSource bool
		wantShouldMark   bool
		explanation      string
	}{
		{
			name:             "stalled checkpoint keeps a legitimate leader from advertising rewind_ready",
			rewindReadyRow:   "f", // control-file checkpoint timeline lags the running timeline
			wantRewindSource: false,
			wantShouldMark:   false,
			explanation:      "the MUL-1004 state: promote succeeded, node is the leader, but no checkpoint has landed on the new timeline",
		},
		{
			name:             "once a checkpoint lands on the new timeline the same leader advertises rewind_ready",
			rewindReadyRow:   "t", // checkpoint timeline caught up to the running timeline
			wantRewindSource: true,
			wantShouldMark:   true,
			explanation:      "recovery: the deferred checkpoint completed, so the gate opens and the follower can pg_rewind",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, mockQS := newTestManagerWithMock(t, "default", "0-inf")
			mockQS.AddQueryPattern(checkpointQueryPattern,
				mock.MakeQueryResult([]string{"rewind_source_ready"}, [][]any{{tt.rewindReadyRow}}))

			// Isolate the stalled checkpoint as the only variable: the node is the
			// rule leader (role passed below), it has not resigned, and it has not
			// already advertised rewind_ready. These are the other three inputs to
			// shouldMarkRewindReady; assert them so a future change to the default
			// test-consensus state can't silently make this test vacuous.
			require.Zero(t, pm.consensusMgr.ResignedLeaderAtTerm(),
				"precondition: leader must not have resigned")
			require.False(t, pm.consensusMgr.GetReplicationPrimary().GetRewindReady(),
				"precondition: leader must not have already advertised rewind_ready")

			// The manager's own SQL pipeline observes the stall through the mock.
			ready, err := pm.rewindSourceReady(context.Background())
			require.NoError(t, err)
			require.Equal(t, tt.wantRewindSource, ready, tt.explanation)

			// The monitor's rewind-ready decision, evaluated for the legitimate
			// leader. This is exactly the predicate MonitorPostgres uses:
			//   if pm.shouldMarkRewindReady(state, role) { markRewindReady }
			state := postgresState{rewindSourceReady: ready}
			got := pm.shouldMarkRewindReady(state, commonconsensus.ConsensusRoleLeader)
			require.Equal(t, tt.wantShouldMark, got, tt.explanation)
		})
	}
}
