// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// makeLifecycleSnapshot builds a snapshot whose AvailabilityStatus carries the
// given LifecycleStatus. The Status itself is minimal — these tests don't care
// about the rest of the health state.
//
// No ConsensusStatus is attached. Tests that need consensus.LeaderTerm to
// return non-zero on the cached state (e.g. to exercise
// synthesizeRequestingDemotion) should use makeLifecycleSnapshotForPrimaryTerm
// which embeds a self-as-leader ConsensusStatus at the given term.
func makeLifecycleSnapshot(signal clustermetadata.LifecycleSignal, deadline time.Duration) *multipoolermanagerdatapb.ManagerHealthStreamResponse {
	return buildLifecycleSnapshot(signal, deadline, nil)
}

// makeLifecycleSnapshotForPrimaryTerm is makeLifecycleSnapshot with a
// ConsensusStatus whose Id and Rule.LeaderId both reference poolerID, and
// whose CoordinatorTerm is the given primaryTerm. This is the shape that
// consensus.LeaderTerm reads as "this pooler is leader at term=primaryTerm."
func makeLifecycleSnapshotForPrimaryTerm(poolerID *clustermetadata.ID, signal clustermetadata.LifecycleSignal, deadline time.Duration, primaryTerm int64) *multipoolermanagerdatapb.ManagerHealthStreamResponse {
	cs := &clustermetadata.ConsensusStatus{
		Id: poolerID,
		CurrentPosition: &clustermetadata.PoolerPosition{
			Rule: &clustermetadata.ShardRule{
				LeaderId: poolerID,
				RuleNumber: &clustermetadata.RuleNumber{
					CoordinatorTerm: primaryTerm,
				},
			},
		},
	}
	return buildLifecycleSnapshot(signal, deadline, cs)
}

func buildLifecycleSnapshot(signal clustermetadata.LifecycleSignal, deadline time.Duration, cs *clustermetadata.ConsensusStatus) *multipoolermanagerdatapb.ManagerHealthStreamResponse {
	lc := &clustermetadata.LifecycleStatus{Signal: signal}
	if deadline > 0 {
		lc.ShutdownDeadline = durationpb.New(deadline)
	}
	return &multipoolermanagerdatapb.ManagerHealthStreamResponse{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamResponse_Snapshot{
			Snapshot: &multipoolermanagerdatapb.ManagerHealthSnapshot{
				Status: &multipoolermanagerdatapb.StatusResponse{
					Status: &multipoolermanagerdatapb.Status{
						PoolerType: clustermetadata.PoolerType_PRIMARY,
					},
					AvailabilityStatus: &clustermetadata.AvailabilityStatus{
						LifecycleStatus: lc,
					},
					ConsensusStatus: cs,
				},
			},
		},
	}
}

// streamEntryForPooler returns the live streamEntry for a pooler, blocking
// briefly until it appears. Used by tests that need to inspect timer state
// directly.
func streamEntryForPooler(t *testing.T, sm *HealthStream, poolerID string) *streamEntry {
	t.Helper()
	require.Eventually(t, func() bool {
		sm.mu.Lock()
		_, ok := sm.streams[poolerID]
		sm.mu.Unlock()
		return ok
	}, 2*time.Second, 10*time.Millisecond, "stream entry never appeared for pooler %s", poolerID)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.streams[poolerID]
}

// timerArmed returns whether the entry currently holds an armed shutdown
// timer, taking entry.mu briefly.
func timerArmed(entry *streamEntry) bool {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.shutdownTimer != nil
}

// announcedDeadline returns the announced deadline value held by the entry.
func announcedDeadline(entry *streamEntry) time.Duration {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.announcedDeadline
}

// TestHealthStream_LifecycleSHUTTING_DOWN_ArmsTimer verifies that a
// SHUTTING_DOWN snapshot arms the per-pooler shutdown timer with the
// announced deadline.
func TestHealthStream_LifecycleSHUTTING_DOWN_ArmsTimer(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	announced := constants.MinShutdownDeadline + 5*time.Second
	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, announced)

	// Wait for snapshot to be applied.
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetAvailabilityStatus().GetLifecycleStatus().GetSignal() == clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN
	}, 2*time.Second, 10*time.Millisecond, "SHUTTING_DOWN should appear in cache")

	entry := streamEntryForPooler(t, sm, key)
	assert.True(t, timerArmed(entry), "timer must be armed after SHUTTING_DOWN")
	assert.Equal(t, announced, announcedDeadline(entry), "announced deadline must match the snapshot")
}

// TestHealthStream_LifecycleSTOPPED_CancelsTimer verifies that a STOPPED
// snapshot following SHUTTING_DOWN cancels the armed timer and leaves the
// cache reflecting STOPPED (so the analyzer triggers failover via the
// existing LeaderNeedsReplacement path).
func TestHealthStream_LifecycleSTOPPED_CancelsTimer(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, 30*time.Second)

	entry := streamEntryForPooler(t, sm, key)
	require.Eventually(t, func() bool { return timerArmed(entry) }, 2*time.Second, 10*time.Millisecond, "timer should arm on SHUTTING_DOWN")

	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, 0)

	require.Eventually(t, func() bool { return !timerArmed(entry) }, 2*time.Second, 10*time.Millisecond, "timer should cancel on STOPPED")

	cached, _ := poolerStore.Get(key)
	assert.Equal(t, clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED,
		cached.GetAvailabilityStatus().GetLifecycleStatus().GetSignal())
}

// TestHealthStream_LifecycleDeadline_ClampedToMinimum verifies that an
// announced deadline below MinShutdownDeadline is clamped on receipt.
func TestHealthStream_LifecycleDeadline_ClampedToMinimum(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	// Announce a deadline below the floor.
	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, 1*time.Second)

	entry := streamEntryForPooler(t, sm, key)
	require.Eventually(t, func() bool { return timerArmed(entry) }, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, constants.MinShutdownDeadline, announcedDeadline(entry),
		"deadline must be clamped to the minimum")
}

// TestHealthStream_LifecycleDuplicateSHUTTING_DOWN_SameDeadline verifies that
// a duplicate SHUTTING_DOWN snapshot with an identical deadline does not
// re-arm the timer.
func TestHealthStream_LifecycleDuplicateSHUTTING_DOWN_SameDeadline(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	deadline := 30 * time.Second
	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, deadline)

	entry := streamEntryForPooler(t, sm, key)
	require.Eventually(t, func() bool { return timerArmed(entry) }, 2*time.Second, 10*time.Millisecond)
	originalTimer := entry.shutdownTimer

	// Re-announce SHUTTING_DOWN with the same deadline. The timer must not be
	// replaced — the deadline was fixed at first announcement.
	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, deadline)

	// Give the snapshot time to be processed.
	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetStreamSnapshotsReceived() >= 2
	}, 2*time.Second, 10*time.Millisecond, "second snapshot should be applied")

	entry.mu.Lock()
	current := entry.shutdownTimer
	entry.mu.Unlock()
	assert.Same(t, originalTimer, current, "timer must not be replaced on duplicate SHUTTING_DOWN")
}

// TestHealthStream_LifecycleDuplicateSHUTTING_DOWN_DifferentDeadline verifies
// that a duplicate SHUTTING_DOWN with a different deadline is not honored
// (timer keeps its original arming).
func TestHealthStream_LifecycleDuplicateSHUTTING_DOWN_DifferentDeadline(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	original := 30 * time.Second
	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, original)

	entry := streamEntryForPooler(t, sm, key)
	require.Eventually(t, func() bool { return timerArmed(entry) }, 2*time.Second, 10*time.Millisecond)

	// Re-announce with a different deadline.
	stream.Ch <- makeLifecycleSnapshot(clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, 60*time.Second)

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetStreamSnapshotsReceived() >= 2
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, original, announcedDeadline(entry),
		"announcedDeadline must keep the original value, not the second announcement")
}

// TestHealthStream_StreamEOFAfterSHUTTING_DOWN_SynthesizesSTOPPED verifies
// that a stream EOF after a SHUTTING_DOWN snapshot causes the orchestrator to
// synthesize a local STOPPED into the cached state and stop trying to
// reconnect (skip the exponential reconnect-with-backoff).
func TestHealthStream_StreamEOFAfterSHUTTING_DOWN_SynthesizesSTOPPED(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 4)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 5)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	stream.Ch <- makeLifecycleSnapshotForPrimaryTerm(poolerID, clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, 30*time.Second, 5)

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetAvailabilityStatus().GetLifecycleStatus().GetSignal() == clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN
	}, 2*time.Second, 10*time.Millisecond)

	// Close the stream; orchestrator should synthesize STOPPED and skip backoff.
	close(stream.Ch)

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetAvailabilityStatus().GetLifecycleStatus().GetSignal() == clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED
	}, 2*time.Second, 10*time.Millisecond, "EOF after SHUTTING_DOWN must synthesize STOPPED")

	// End-to-end check: the orchestrator's REQUESTING_DEMOTION synthesis must
	// drive the analyzer-facing helper to true, so failover proceeds even
	// though the orchestrator never received an explicit STOPPED snapshot.
	cached, _ := poolerStore.Get(key)
	assert.True(t, types.LeaderNeedsReplacement(cached),
		"after EOF-triggered REQUESTING_DEMOTION, LeaderNeedsReplacement must trigger failover")

	// No reconnection should be attempted: streamCh stays empty.
	select {
	case <-streamCh:
		t.Fatal("orchestrator should not reconnect after SHUTTING_DOWN + EOF")
	case <-time.After(500 * time.Millisecond):
	}
}

// TestHealthStream_StreamEOFWithoutLifecycleSignal_KeepsBackoff verifies that
// an EOF on a stream that never announced a lifecycle signal still goes
// through the existing reconnect-with-backoff path.
func TestHealthStream_StreamEOFWithoutLifecycleSignal_KeepsBackoff(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 4)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	// Send a regular snapshot — no lifecycle signal.
	stream.Ch <- makeSnapshot(&multipoolermanagerdatapb.Status{
		PoolerType:    clustermetadata.PoolerType_PRIMARY,
		PostgresReady: true,
	})

	// Close the stream. Orchestrator must reconnect.
	close(stream.Ch)

	select {
	case <-streamCh:
		// Expected: a reconnect attempt arrived.
	case <-time.After(3 * time.Second):
		t.Fatal("expected reconnect attempt after spurious EOF without lifecycle signal")
	}
}

// TestHealthStream_OnShutdownDeadlineExpired_SynthesizesSTOPPED tests the
// timer-callback function in isolation: when the cache does not yet show
// STOPPED, the callback writes a synthesized STOPPED into the cache.
func TestHealthStream_OnShutdownDeadlineExpired_SynthesizesSTOPPED(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 5)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)

	// Build a fake entry with a no-op cancel — we don't actually arm a real timer.
	entry := &streamEntry{cancel: func() {}}

	hs.onShutdownDeadlineExpired(key, entry)

	cached, ok := poolerStore.Get(key)
	require.True(t, ok)
	assert.Equal(t, clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED,
		cached.GetAvailabilityStatus().GetLifecycleStatus().GetSignal(),
		"deadline expiry must synthesize STOPPED into the cache")

	// End-to-end check: the orchestrator's REQUESTING_DEMOTION synthesis must
	// drive the analyzer-facing helper to true, so failover proceeds even
	// though the orchestrator never received an explicit STOPPED snapshot or
	// stream EOF.
	assert.True(t, types.LeaderNeedsReplacement(cached),
		"after deadline-triggered REQUESTING_DEMOTION, LeaderNeedsReplacement must trigger failover")
}

// TestHealthStream_LeaderNotReplacedWhileShuttingDown verifies the integration
// property that matters for split-brain safety: while a pooler has announced
// SHUTTING_DOWN but the stream is still alive and no STOPPED snapshot has
// arrived, LeaderNeedsReplacement on the cached state must return false. The
// orchestrator must NOT trigger failover during the smart-shutdown window
// while Postgres may still be serving in-flight writes.
//
// Once STOPPED arrives (or the stream EOFs / the deadline expires), the
// helper flips to true and failover proceeds. This test only covers the
// "STOPPED arrives normally" path; the EOF and deadline-expiry paths are
// covered by their dedicated tests above.
func TestHealthStream_LeaderNotReplacedWhileShuttingDown(t *testing.T) {
	ctx := t.Context()

	fakeClient := rpcclient.NewFakeClient()
	streamCh := make(chan *rpcclient.FakeManagerHealthStream, 1)
	fakeClient.OnManagerHealthStream = func(_ string, s *rpcclient.FakeManagerHealthStream) {
		streamCh <- s
	}

	poolerStore := store.NewPoolerStore(fakeClient, slog.Default())
	sm := newTestHealthStream(ctx, fakeClient, poolerStore)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 5)

	sm.Start(poolerID)
	stream := <-streamCh
	completeHandshake(t, stream)

	// Pooler announces SHUTTING_DOWN but does not (yet) send STOPPED.
	stream.Ch <- makeLifecycleSnapshotForPrimaryTerm(poolerID, clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN, 30*time.Second, 5)

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetAvailabilityStatus().GetLifecycleStatus().GetSignal() == clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN
	}, 2*time.Second, 10*time.Millisecond, "SHUTTING_DOWN should appear in cache")

	// Critical assertion: the analyzer-facing helper must NOT consider the
	// leader as needing replacement. Failover during this window would race
	// against the still-running Postgres on the old leader. The orchestrator
	// does not synthesize REQUESTING_DEMOTION on SHUTTING_DOWN alone — only
	// on the definitive STOPPED / EOF / deadline-expiry events.
	cached, ok := poolerStore.Get(key)
	require.True(t, ok)
	assert.False(t, types.LeaderNeedsReplacement(cached),
		"LeaderNeedsReplacement must be false while pooler is in SHUTTING_DOWN with stream still alive")

	// Now the pooler completes its shutdown and sends STOPPED.
	stream.Ch <- makeLifecycleSnapshotForPrimaryTerm(poolerID, clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED, 0, 5)

	require.Eventually(t, func() bool {
		s, ok := poolerStore.Get(key)
		return ok && s.GetAvailabilityStatus().GetLeadershipStatus().GetSignal() == clustermetadata.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION
	}, 2*time.Second, 10*time.Millisecond, "STOPPED snapshot should synthesize REQUESTING_DEMOTION for the primary")

	// Now LeaderNeedsReplacement should return true; failover proceeds.
	cached, _ = poolerStore.Get(key)
	assert.True(t, types.LeaderNeedsReplacement(cached),
		"LeaderNeedsReplacement must be true after STOPPED arrives and REQUESTING_DEMOTION is synthesized")
}

// TestHealthStream_OnShutdownDeadlineExpired_SkipsIfAlreadyStopped tests that
// the deadline callback is idempotent: if the cache already reflects STOPPED
// (because a snapshot or EOF beat the timer), the callback does nothing.
func TestHealthStream_OnShutdownDeadlineExpired_SkipsIfAlreadyStopped(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := topoclient.MultiPoolerIDString(poolerID)
	poolerStore.Set(key, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadata.MultiPooler{
			Id:         poolerID,
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadata.PoolerType_PRIMARY,
		},
		AvailabilityStatus: &clustermetadata.AvailabilityStatus{
			LifecycleStatus: &clustermetadata.LifecycleStatus{
				Signal: clustermetadata.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED,
			},
		},
	})

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)

	// cancel records whether entry.cancel is called.
	cancelled := false
	entry := &streamEntry{cancel: func() { cancelled = true }}

	hs.onShutdownDeadlineExpired(key, entry)

	assert.False(t, cancelled, "cancel must not be called when STOPPED is already cached")
}

// TestSynthesizeRequestingDemotion_SetsSignalForPrimary verifies the
// happy-path: a PRIMARY pooler with a non-zero primary_term gets
// REQUESTING_DEMOTION written into its cached LeadershipStatus with the
// captured term.
func TestSynthesizeRequestingDemotion_SetsSignalForPrimary(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 7)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "test")

	cached, ok := poolerStore.Get(key)
	require.True(t, ok)
	leadership := cached.GetAvailabilityStatus().GetLeadershipStatus()
	require.NotNil(t, leadership, "LeadershipStatus must be written")
	assert.Equal(t, clustermetadata.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION, leadership.Signal)
	assert.Equal(t, int64(7), leadership.LeaderTerm,
		"leader_term must equal the captured primary_term")
	assert.True(t, types.LeaderNeedsReplacement(cached),
		"LeaderNeedsReplacement must fire after REQUESTING_DEMOTION is synthesized")
}

// TestSynthesizeRequestingDemotion_NoOpForNonPrimary verifies that a
// REPLICA pooler does not get REQUESTING_DEMOTION written even when the
// observer's trigger fires. Only the leader's departure should request
// demotion.
func TestSynthesizeRequestingDemotion_NoOpForNonPrimary(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_REPLICA)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "test")

	cached, _ := poolerStore.Get(key)
	assert.Nil(t, cached.GetAvailabilityStatus().GetLeadershipStatus(),
		"non-primary must not get a LeadershipStatus written")
}

// TestSynthesizeRequestingDemotion_NoOpForZeroTerm verifies that a PRIMARY
// with no consensus primary_term (or term==0) is left alone. Writing
// REQUESTING_DEMOTION with leader_term=0 would never pass LeaderNeedsReplacement's
// staleness check, so the helper short-circuits.
func TestSynthesizeRequestingDemotion_NoOpForZeroTerm(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	// PRIMARY type but no ConsensusStatus → primary_term == 0.
	key := seedPooler(poolerStore, poolerID, clustermetadata.PoolerType_PRIMARY)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "test")

	cached, _ := poolerStore.Get(key)
	assert.Nil(t, cached.GetAvailabilityStatus().GetLeadershipStatus(),
		"primary with zero term must not get a LeadershipStatus written")
}

// TestSynthesizeRequestingDemotion_IdempotentSameTerm verifies that the
// signal stays at the same term across repeated calls. Convergent trigger
// paths (STOPPED snapshot + EOF + deadline expiry) can all fire for the same
// pooler; the helper must short-circuit so the cached signal does not get
// rewritten with a stale value.
func TestSynthesizeRequestingDemotion_IdempotentSameTerm(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	fakeClient := rpcclient.NewFakeClient()
	poolerStore := store.NewPoolerStore(fakeClient, logger)

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"}
	key := seedPrimaryWithTerm(poolerStore, poolerID, 7)

	hs := NewHealthStream(context.Background(), fakeClient, poolerStore, logger)
	hs.synthesizeRequestingDemotion(key, "first")
	hs.synthesizeRequestingDemotion(key, "second")
	hs.synthesizeRequestingDemotion(key, "third")

	cached, _ := poolerStore.Get(key)
	leadership := cached.GetAvailabilityStatus().GetLeadershipStatus()
	require.NotNil(t, leadership)
	assert.Equal(t, clustermetadata.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION, leadership.Signal)
	assert.Equal(t, int64(7), leadership.LeaderTerm)
}
