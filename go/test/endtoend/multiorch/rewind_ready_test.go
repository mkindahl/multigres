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

package multiorch

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestLeaderNeverBecomesRewindReadyAfterFailover reproduces the ROOT of MUL-1004:
// after a failover, the new leader never advertises rewind_ready, so followers
// can never pg_rewind onto it and the shard never converges.
//
// This does NOT inject rewind_ready. It triggers the condition naturally: with a
// small promotion timeout (set at runtime after bootstrap, via SetParameters),
// waitForPromotionComplete abandons the promotion on its internal deadline even
// though pg_promote() has already run (a point of no return: postgres forks to
// the new timeline and comes up as a live primary in ~20ms). From consensus's
// view the promote "failed", so multiorch never records that node as leader and
// cascades into re-appointing others ("Promote succeeded ... is_leader:false").
//
// The abandoned node is now a real primary on the new timeline whose checkpoint
// HAS completed (rewindSourceReady == true), yet it never advertises
// rewind_ready: shouldMarkRewindReady gates on role == ConsensusRoleLeader, and
// after the cascade the rule no longer names this node the leader. Nothing then
// demotes it either, because stale-primary demotion is itself gated on the new
// leader being rewind_ready. So the checkpoint is NOT the blocker here — the
// consensus role gate is. (The EKS bundle showed the complementary symptom, a
// frozen LSN / checkpoint that never ran; both dead-end at "no rewind_ready".)
//
// The test observes each pooler's real rewind_ready (Manager.Status ->
// ConsensusStatus) and its checkpoint-vs-current timeline (direct SQL), and
// asserts that some leader becomes rewind_ready within the window. On the EKS
// build (c675c93e) it fails — no leader ever advertises rewind_ready.
func TestLeaderNeverBecomesRewindReadyAfterFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestLeaderNeverBecomesRewindReadyAfterFailover in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithLeaderFailoverGracePeriod("0s", "0s"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	oldPrimaryName := setup.PrimaryName
	primary := setup.GetMultipoolerInstance(oldPrimaryName)
	require.NotNil(t, primary)

	// Bootstrap already promoted the initial primary with the default 30s timeout.
	// Now shrink it so the FAILOVER promotion abandons before it can issue the
	// post-promotion CHECKPOINT that sets rewind_ready.
	for _, inst := range setup.Multipoolers {
		setPromotionTimeout(t, inst, "10ms")
	}
	setRestarts(t, primary, false) // keep the killed primary down

	setup.KillPostgres(t, oldPrimaryName)

	// Observe: does any pooler ever advertise rewind_ready? Log rewind_ready and
	// the checkpoint-vs-current timeline for each pooler each second.
	sawRewindReady := false
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && !sawRewindReady {
		for name, inst := range setup.Multipoolers {
			rr, haveCS := poolerRewindReady(t, inst)
			ck := checkpointState(inst)
			t.Logf("%s: rewind_ready=%v (consensus_status=%v)  %s", name, rr, haveCS, ck)
			if rr {
				sawRewindReady = true
			}
		}
		if !sawRewindReady {
			time.Sleep(1 * time.Second)
		}
	}

	require.Truef(t, sawRewindReady,
		"no leader advertised rewind_ready within 60s — an abandoned promotion (pg_promote already ran) left an "+
			"orphan primary that consensus no longer names as leader, so shouldMarkRewindReady's leader gate never "+
			"passes; followers can never pg_rewind/rejoin and the orphan is never demoted (MUL-1004 root)")
}

// poolerRewindReady reads a pooler's self-advertised rewind_ready from its
// consensus status. Returns (rewind_ready, consensus-status-available).
func poolerRewindReady(t *testing.T, inst *shardsetup.MultipoolerInstance) (bool, bool) {
	t.Helper()
	c, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	if err != nil {
		return false, false
	}
	defer c.Close()
	resp, err := c.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil || resp.GetConsensusStatus() == nil {
		return false, false
	}
	rp := commonconsensus.ReplicationPrimaryOrNil(resp.GetConsensusStatus())
	return rp.GetRewindReady(), true
}

// checkpointState reports a pooler's control-file checkpoint timeline vs its
// current running timeline. rewindSourceReady is true only when they match
// (a checkpoint has run on the current timeline).
func checkpointState(inst *shardsetup.MultipoolerInstance) string {
	socketDir := filepath.Join(inst.Pgctld.PoolerDir, "pg_sockets")
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable password=%s connect_timeout=2",
		socketDir, inst.Pgctld.PgPort, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return "pg=unreachable"
	}
	defer db.Close()
	var inRecovery bool
	var ckptTLI, curTLI int
	err = db.QueryRow(`SELECT pg_is_in_recovery(),
		(SELECT timeline_id FROM pg_control_checkpoint()),
		CASE WHEN pg_is_in_recovery()
		     THEN ('x'||substring(pg_walfile_name(pg_last_wal_replay_lsn()) from 1 for 8))::bit(32)::int
		     ELSE ('x'||substring(pg_walfile_name(pg_current_wal_lsn()) from 1 for 8))::bit(32)::int
		END`).Scan(&inRecovery, &ckptTLI, &curTLI)
	if err != nil {
		return "pg=unreachable"
	}
	return fmt.Sprintf("in_recovery=%v ckpt_tli=%d cur_tli=%d rewindSourceReady=%v",
		inRecovery, ckptTLI, curTLI, !inRecovery && ckptTLI == curTLI)
}
