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

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestCascadeReelectionLeavesDualPrimary reproduces MUL-1004 end-to-end: after
// a primary failure whose promotion takes longer than the promotion timeout, a
// cascade of re-elections leaves the shard with two write-capable primaries
// (split-brain) instead of converging to one.
//
// Trigger: bootstrap runs with the default 30s promotion timeout, so cluster
// setup is unaffected. Immediately before the kill we lower the timeout to 10ms
// on every pooler via the SetPromotionTimeout RPC. 10ms is below the 100ms
// promotion poll tick, so the failover promotion's wait expires before postgres
// finishes coming up — deterministically, regardless of how fast the (tiny)
// local promotion is. This is the runtime equivalent of "WAL replay took longer
// than the promotion timeout", with no real replay backlog or sleep.
//
// On the EKS build (c675c93e) waitForPromotionComplete then abandons the
// promotion while postgres is still finishing, clears POSTGRES_STATUS_PROMOTING,
// and multiorch fires another election. Each surviving standby gets
// pg_promote()d, and because the cascade never settles the rewind/demote path
// (gated on the leader advertising rewind_ready) never fires — so both come up
// write-capable and stay that way. The assertion below (never more than one
// write-capable pooler) catches that dual-primary split-brain.
//
// EXPECTED: fails on c675c93e (observes 2 write-capable poolers); passes on a
// build that does not abandon an in-flight promotion on an internal deadline.
func TestCascadeReelectionLeavesDualPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestCascadeReelectionLeavesDualPrimary in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithMultigateway(),
		// No grace: multiorch re-elects immediately when it sees the leader is
		// not PROMOTING and not ready, maximizing the chance a competing election
		// fires inside the (short) window the abandoned promotion opens.
		shardsetup.WithLeaderFailoverGracePeriod("0s", "0s"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	oldPrimaryName := setup.PrimaryName
	primary := setup.GetMultipoolerInstance(oldPrimaryName)
	require.NotNil(t, primary)

	// Lower the promotion timeout below the 100ms poll tick on every pooler, so
	// the failover promotion is abandoned before postgres finishes coming up.
	// Bootstrap already happened with the default 30s, so it was unaffected.
	for _, inst := range setup.Multipoolers {
		setPromotionTimeout(t, inst, "10ms")
	}

	// Keep the killed primary down so the write-capable count reflects the
	// standby cascade, not a bouncing old primary.
	setRestarts(t, primary, false)

	// AZ LOSS: kill the primary to trigger failover.
	setup.KillPostgres(t, oldPrimaryName)

	// Let multiorch attempt the (now-abandoned) failover promotion, then restore
	// the AZ: the old primary's postgres comes back. On the buggy build the new
	// leader never settles/advertises rewind_ready, so the returning old primary
	// is never demoted and stays write-capable alongside a promoted standby.
	time.Sleep(5 * time.Second)
	setRestarts(t, primary, true)

	// Poll the whole shard and require it to CONVERGE to a single write-capable
	// primary. A correct build demotes the returning old primary within a few
	// seconds (a brief two-writable overlap during demotion is fine); the buggy
	// build never settles because the new leader never advertises rewind_ready,
	// so two primaries stay write-capable indefinitely.
	//
	// Convergence = 10 consecutive samples (~10s) with <=1 write-capable pooler.
	const convergedStreak = 10
	streak := 0
	var lastNames []string
	converged := false
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		w, names := writableProbe(t, setup)
		if w >= 2 {
			streak = 0
			lastNames = names
			t.Logf("write-capable poolers: %d %v", w, names)
		} else {
			streak++
			if streak >= convergedStreak {
				converged = true
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	require.Truef(t, converged,
		"shard never converged to a single write-capable primary within 90s — two primaries stayed write-capable (%v): dual primary / split-brain (MUL-1004)",
		lastNames)
}

// setPromotionTimeout lowers a pooler's promotion-completion wait timeout at
// runtime via the generic SetParameters RPC.
func setPromotionTimeout(t *testing.T, inst *shardsetup.MultipoolerInstance, dur string) {
	t.Helper()
	c, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Manager.SetParameters(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetParametersRequest{
			Parameters: map[string]string{"promotion_timeout": dur},
		})
	require.NoError(t, err)
}

// setRestarts toggles pgctld's automatic postgres restarts on a pooler.
func setRestarts(t *testing.T, inst *shardsetup.MultipoolerInstance, enabled bool) {
	t.Helper()
	c, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer c.Close()
	_, err = c.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: enabled})
	require.NoError(t, err)
}

// writableProbe returns how many reachable poolers are locally write-capable and
// their names, using a rollback-only temp-table probe (the same shape as the
// evidence bundle's local-write probes). Unreachable poolers (e.g. the killed
// primary, or a node mid-restart during a cascade) are tolerated and counted as
// not writable — we must not fail the test just because a node is down.
func writableProbe(t *testing.T, setup *shardsetup.ShardSetup) (int, []string) {
	t.Helper()
	writable := 0
	var names []string
	for name, inst := range setup.Multipoolers {
		if probeLocallyWritable(inst) {
			writable++
			names = append(names, name)
		}
	}
	return writable, names
}

func probeLocallyWritable(inst *shardsetup.MultipoolerInstance) bool {
	socketDir := filepath.Join(inst.Pgctld.PoolerDir, "pg_sockets")
	connStr := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable password=%s connect_timeout=2",
		socketDir, inst.Pgctld.PgPort, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return false
	}
	defer db.Close()
	var inRecovery bool
	if err := db.QueryRow(`SELECT pg_is_in_recovery()`).Scan(&inRecovery); err != nil {
		return false // unreachable / down: not counted
	}
	if inRecovery {
		return false
	}
	tx, err := db.Begin()
	if err != nil {
		return false
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(fmt.Sprintf("CREATE TEMP TABLE mul1004_write_probe_%d (id int)", time.Now().UnixNano()))
	return err == nil
}
