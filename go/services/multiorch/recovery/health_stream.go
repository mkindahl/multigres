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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/retry"
)

const (
	// streamReconnectInitialBackoff is the initial wait before retrying a
	// failed ManagerHealthStream connection.
	streamReconnectInitialBackoff = 1 * time.Second

	// streamReconnectMaxBackoff caps the exponential backoff between retries.
	streamReconnectMaxBackoff = 30 * time.Second
)

// streamEntry holds the lifecycle handles for one active stream goroutine.
type streamEntry struct {
	cancel context.CancelFunc

	// mu protects stream and the shutdown-timer fields below. stream is set to
	// the live gRPC stream after the start message is sent and cleared on
	// stream exit.
	mu     sync.Mutex
	stream rpcclient.ManagerHealthStream

	// shutdownTimer is armed when this pooler announces SHUTTING_DOWN and is
	// cancelled on STOPPED, on stream EOF, or when the entry is torn down. On
	// expiry the callback synthesizes a local STOPPED into the pooler's cached
	// PoolerHealthState so failover proceeds via the analyzer's normal path
	// even when the orchestrator has lost contact with the pooler.
	shutdownTimer *time.Timer

	// announcedDeadline is the (possibly clamped) deadline value that armed the
	// timer. Used to detect duplicate SHUTTING_DOWN snapshots that announce a
	// different deadline (a buggy or misconfigured pooler).
	announcedDeadline time.Duration
}

// HealthStream maintains one ManagerHealthStream stream per pooler. It replaces
// the polling loop: instead of periodically calling the Status RPC, each pooler
// pushes health snapshots to multiorch via a long-lived gRPC stream.
//
// When a snapshot arrives the HealthStream writes the same health fields into
// the pooler store that the old pollPooler function wrote on success. On stream
// disconnect the pooler is marked unreachable and reconnection is attempted
// with exponential backoff (1s → 2s → … → 30s cap).
//
// The orchestrator sends its preferred snapshot_interval and staleness_timeout
// in the start message. The server echoes back the actual values it will use in
// a ManagerHealthStreamStartResponse, which the client uses to arm its staleness
// watchdog.
type HealthStream struct {
	logger    *slog.Logger
	rpcClient rpcclient.MultiPoolerClient
	store     *store.PoolerStore

	// snapshotInterval is requested from the server as the proactive snapshot
	// tick rate. Zero means use the server default (currently 5s).
	snapshotInterval time.Duration

	// stalenessTimeout is sent to the server and used to arm the staleness
	// watchdog (seeded from the start response). Zero means server default
	// (timeouts.DefaultHealthStreamStalenessTimeout).
	stalenessTimeout time.Duration

	// Active stream goroutines, keyed by pooler ID string.
	mu      sync.Mutex
	streams map[string]*streamEntry

	// Parent context; cancelled by Stop().
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Option is a functional option for NewHealthStream.
type Option func(*HealthStream)

// WithSnapshotInterval sets the proactive snapshot interval sent to the server
// in the start message.
func WithSnapshotInterval(d time.Duration) Option {
	return func(hs *HealthStream) {
		hs.snapshotInterval = d
	}
}

// WithStalenessTimeout sets the staleness timeout sent to the server and used
// to arm the client-side staleness watchdog. Intended for tests.
func WithStalenessTimeout(d time.Duration) Option {
	return func(hs *HealthStream) {
		hs.stalenessTimeout = d
	}
}

func (entry *streamEntry) withLock(action func()) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	action()
}

// armTimer arms the per-pooler shutdown deadline timer on the first
// SHUTTING_DOWN announcement. On expiry the callback synthesizes a local
// LIFECYCLE_SIGNAL_STOPPED into the cached PoolerHealthState so the analyzer
// can fail over without contact with the pooler.
//
// On duplicate SHUTTING_DOWN announcements (timer already armed) the original
// deadline is kept; if the duplicate announces a different value a warning is
// logged (a buggy or misconfigured pooler signal). Identical re-announcements
// are silent.
//
// Caller is expected to have already clamped deadline to MinShutdownDeadline.
func (entry *streamEntry) armTimer(ctx context.Context, hs *HealthStream, poolerID string, deadline time.Duration) {
	entry.withLock(func() {
		if entry.shutdownTimer != nil {
			if deadline != entry.announcedDeadline {
				hs.logger.WarnContext(ctx, "duplicate SHUTTING_DOWN with mismatched deadline; keeping original",
					"pooler_id", poolerID,
					"original", entry.announcedDeadline,
					"received", deadline)
			}
			return
		}

		entry.announcedDeadline = deadline
		entry.shutdownTimer = time.AfterFunc(deadline, func() {
			hs.onShutdownDeadlineExpired(poolerID, entry)
		})
		hs.logger.InfoContext(ctx, "armed shutdown deadline timer",
			"pooler_id", poolerID,
			"deadline", deadline)
	})
}

func (entry *streamEntry) stopTimer() {
	entry.withLock(func() {
		if entry.shutdownTimer != nil {
			entry.shutdownTimer.Stop()
			entry.shutdownTimer = nil
		}
	})
}

// NewHealthStream creates a HealthStream.
//
// Call Start() for each pooler that should be monitored.
func NewHealthStream(
	ctx context.Context,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	logger *slog.Logger,
	options ...Option,
) *HealthStream {
	smCtx, cancel := context.WithCancel(ctx)
	hs := &HealthStream{
		logger:    logger,
		rpcClient: rpcClient,
		store:     poolerStore,
		streams:   make(map[string]*streamEntry),
		ctx:       smCtx,
		cancel:    cancel,
	}

	for _, opt := range options {
		opt(hs)
	}

	return hs
}

// Shutdown cancels all active streams and waits for their goroutines to exit.
func (hs *HealthStream) Shutdown() {
	hs.cancel()
	hs.wg.Wait()
}

// Start starts a health stream for id.
// If a stream is already running for this pooler the call is a no-op.
// The pooler's MultiPooler metadata is read from the store on each
// reconnect attempt so topology updates are automatically picked up.
func (hs *HealthStream) Start(id *clustermetadatapb.ID) {
	poolerID := topoclient.MultiPoolerIDString(id)
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if _, exists := hs.streams[poolerID]; exists {
		return
	}

	ctx, cancel := context.WithCancel(hs.ctx)
	entry := &streamEntry{cancel: cancel}
	hs.streams[poolerID] = entry

	hs.wg.Go(func() {
		defer func() {
			hs.mu.Lock()
			delete(hs.streams, poolerID)
			hs.mu.Unlock()

			// Cancel any pending shutdown deadline timer; the stream is gone
			// and the timer goroutine would otherwise still hold a reference to
			// this entry past its useful lifetime.
			entry.stopTimer()
		}()
		hs.runStream(ctx, poolerID, entry)
	})
}

// Stop the health stream for a pooler.
//
// The stream goroutine will exit and the pooler will be marked unreachable
// until a new stream is started. The pooler's MultiPooler metadata must remain
// in the store for the stream to reconnect if Start() is called again.
//
// If no stream is running for this pooler the call is a no-op.
func (hs *HealthStream) Stop(id *clustermetadatapb.ID) {
	poolerID := topoclient.MultiPoolerIDString(id)
	hs.mu.Lock()
	defer hs.mu.Unlock()

	// The goroutine removes itself from hs.streams via its defer so we
	// don't need to delete the entry here; just cancel it and let the
	// goroutine clean up.
	if entry, exists := hs.streams[poolerID]; exists {
		entry.cancel()
	}
}

// runStream manages the lifecycle of one stream, reconnecting with backoff on failure.
// It reads the latest MultiPooler metadata from the store on each reconnect attempt
// so hostname/port changes are picked up automatically.
func (hs *HealthStream) runStream(ctx context.Context, poolerID string, entry *streamEntry) {
	r := retry.New(streamReconnectInitialBackoff, streamReconnectMaxBackoff, retry.WithInitialDelay())
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			return
		}

		// Read current pooler metadata from store on every attempt.
		poolerHealth, ok := hs.store.Get(poolerID)
		if !ok || poolerHealth.MultiPooler == nil {
			hs.logger.WarnContext(ctx, "pooler not found in store, stopping health stream",
				"pooler_id", poolerID)
			return
		}

		connected, streamErr := hs.streamOnce(ctx, poolerID, poolerHealth, entry)
		if ctx.Err() != nil {
			return
		}

		if connected {
			// Stream was successfully established before failing — reset backoff.
			r.Reset()
		}

		hs.markDisconnected(poolerID)

		if streamErr != nil {
			hs.logger.WarnContext(ctx, "health stream disconnected",
				"pooler_id", poolerID,
				"error", streamErr,
			)
		}

		// If the pooler announced graceful shutdown before the stream ended,
		// stop trying to reconnect: it is intentionally going away. For a
		// SHUTTING_DOWN pooler, also synthesize a local STOPPED so the analyzer
		// can fail over without waiting for the deadline timer (the EOF is
		// itself the strongest evidence the pooler has left).
		switch hs.lastLifecycleSignal(poolerID) {
		case clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN:
			reason := "stream EOF after SHUTTING_DOWN"
			entry.stopTimer()
			hs.synthesizeShutdownStopped(poolerID, reason)
			hs.synthesizeRequestingDemotion(poolerID, reason)
			return
		case clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED:
			// Cache already shows STOPPED; no synthesis needed. Skip backoff.
			reason := "stream EOF after STOPPED"
			hs.synthesizeRequestingDemotion(poolerID, reason)
			return
		}
	}
}

// lastLifecycleSignal returns the LifecycleSignal currently cached for a
// pooler, or UNKNOWN if no AvailabilityStatus or LifecycleStatus is present.
// Used to decide whether a stream EOF should trigger reconnect-with-backoff
// (existing behaviour, no SHUTTING_DOWN/STOPPED in cache) or be treated as
// the end of an intentional shutdown.
func (hs *HealthStream) lastLifecycleSignal(poolerID string) clustermetadatapb.LifecycleSignal {
	cached, ok := hs.store.Get(poolerID)
	if !ok {
		return clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_UNKNOWN
	}
	return cached.GetAvailabilityStatus().GetLifecycleStatus().GetSignal()
}

// streamOnce opens one ManagerHealthStream and reads until the stream fails or
// the context is cancelled. Returns (connected, err): connected is true if the
// stream was established before any error occurred.
func (hs *HealthStream) streamOnce(ctx context.Context, poolerID string, poolerHealth *multiorchdatapb.PoolerHealthState, entry *streamEntry) (connected bool, _ error) {
	// Build the start request, sending the orchestrator's preferred timing.
	// Zero values are omitted so the server uses its own defaults.
	startReq := &multipoolermanagerdatapb.ManagerHealthStreamStartRequest{}
	if hs.snapshotInterval > 0 {
		startReq.SnapshotInterval = durationpb.New(hs.snapshotInterval)
	}
	if hs.stalenessTimeout > 0 {
		startReq.StalenessTimeout = durationpb.New(hs.stalenessTimeout)
	}

	// Seed the staleness watchdog before any message is received. This is the
	// value we sent; the server will confirm (or adjust) it in the start response,
	// at which point we reset the watchdog to the echoed value.
	initialStaleness := timeouts.DefaultHealthStreamStalenessTimeout
	if hs.stalenessTimeout > 0 {
		initialStaleness = hs.stalenessTimeout
	}

	// Staleness watchdog: cancel the stream if no message arrives within the
	// timeout. This catches the "server goroutine stuck but TCP alive" failure
	// mode that gRPC keepalive does not cover.
	//
	// The watchdog context is passed to ManagerHealthStream so cancelling it
	// terminates the gRPC stream and causes stream.Recv() to return an error.
	watchdogCtx, cancelWatchdog := context.WithCancel(ctx)
	defer cancelWatchdog()

	// resetCh carries the new timer duration whenever a message is received.
	// Buffered so the recv loop never blocks on the watchdog goroutine.
	resetCh := make(chan time.Duration, 1)
	go func() {
		current := initialStaleness
		timer := time.NewTimer(current)
		defer timer.Stop()
		for {
			select {
			case d := <-resetCh:
				current = d
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(current)
			case <-timer.C:
				hs.logger.WarnContext(ctx, "health stream stale: no message received within timeout, reconnecting",
					"pooler_id", poolerID,
					"timeout", current,
				)
				cancelWatchdog()
				return
			case <-watchdogCtx.Done():
				return
			}
		}
	}()

	stream, err := hs.rpcClient.ManagerHealthStream(watchdogCtx, poolerHealth.MultiPooler)
	if err != nil {
		return false, fmt.Errorf("open stream: %w", err)
	}

	// Send the start message with the negotiated timing preferences.
	if err := stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Start{
			Start: startReq,
		},
	}); err != nil {
		return false, fmt.Errorf("send start: %w", err)
	}

	// Read the start response — the first server message confirms the actual
	// timing values the server will use.
	firstMsg, err := stream.Recv()
	if err != nil {
		if watchdogCtx.Err() != nil && ctx.Err() == nil {
			return false, errors.New("staleness timeout waiting for start response")
		}
		return false, fmt.Errorf("recv start response: %w", err)
	}
	startResp := firstMsg.GetStart()
	if startResp == nil {
		return false, fmt.Errorf("expected start response, got %T", firstMsg.GetMessage())
	}
	// Determine the effective staleness for the watchdog:
	//   - If a local override is set (WithStalenessTimeout), use it directly.
	//     This preserves sub-second precision used in tests.
	//   - Otherwise, use the server-confirmed value from the start response.
	confirmedStaleness := initialStaleness
	if hs.stalenessTimeout == 0 {
		if s := startResp.StalenessTimeout.AsDuration(); s > 0 {
			confirmedStaleness = s
		}
	}
	select {
	case resetCh <- confirmedStaleness:
	default:
	}

	// Expose the live stream so Poll() can send requests.
	entry.withLock(func() { entry.stream = stream })
	defer entry.withLock(func() { entry.stream = nil })

	hs.markConnected(poolerID)

	for {
		resp, err := stream.Recv()
		if err != nil {
			// Distinguish a staleness-triggered cancellation from an external one
			// so the caller can log a useful error message.
			if watchdogCtx.Err() != nil && ctx.Err() == nil {
				return true, fmt.Errorf("staleness timeout: no snapshot received within %s", confirmedStaleness)
			}
			return true, fmt.Errorf("recv: %w", err)
		}
		if snap := resp.GetSnapshot(); snap != nil {
			// Reset the staleness watchdog. Prefer the local override (which
			// preserves sub-second precision); fall back to the server-echoed value.
			timeout := confirmedStaleness
			if hs.stalenessTimeout == 0 {
				if s := snap.Timeout.AsDuration(); s > 0 {
					timeout = s
				}
			}
			select {
			case resetCh <- timeout:
			default:
				// A reset is already pending; the watchdog will pick it up.
			}
			hs.applySnapshot(ctx, poolerID, poolerHealth, snap, entry)
		}
	}
}

// Poll sends a poll request on the active stream for poolerID, triggering an
// immediate health snapshot from the pooler. Returns an error if no stream is
// active or the send fails.
func (hs *HealthStream) Poll(id *clustermetadatapb.ID) error {
	poolerID := topoclient.MultiPoolerIDString(id)
	hs.mu.Lock()
	entry, exists := hs.streams[poolerID]
	hs.mu.Unlock()
	if !exists {
		return fmt.Errorf("no active stream for pooler %s", poolerID)
	}

	entry.mu.Lock()
	stream := entry.stream
	entry.mu.Unlock()
	if stream == nil {
		return fmt.Errorf("stream not yet established for pooler %s", poolerID)
	}

	return stream.Send(&multipoolermanagerdatapb.ManagerHealthStreamClientMessage{
		Message: &multipoolermanagerdatapb.ManagerHealthStreamClientMessage_Poll{
			Poll: &multipoolermanagerdatapb.ManagerHealthStreamPollRequest{},
		},
	})
}

// applySnapshot writes health fields from a snapshot into the pooler store.
// This mirrors the field writes performed by the old pollPooler function on
// success.
//
// After the cache write, applySnapshot inspects the just-arrived
// LifecycleStatus and arms or cancels the per-pooler shutdown deadline timer
// (entry.shutdownTimer). See handleLifecycleSnapshot for the rules.
func (hs *HealthStream) applySnapshot(ctx context.Context, poolerID string, poolerHealth *multiorchdatapb.PoolerHealthState, snapshot *multipoolermanagerdatapb.ManagerHealthSnapshot, entry *streamEntry) {
	if snapshot.Status == nil || snapshot.Status.Status == nil {
		hs.logger.WarnContext(ctx, "received snapshot with nil status, skipping",
			"pooler_id", poolerID)
		return
	}

	status := snapshot.Status.Status
	now := timestamppb.Now()

	poolerIDStr := topoclient.MultiPoolerIDString(poolerHealth.MultiPooler.Id)
	update := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		existing.LastCheckSuccessful = now
		existing.LastSeen = now
		existing.IsUpToDate = true
		existing.IsLastCheckValid = true
		existing.Status = proto.Clone(status).(*multipoolermanagerdatapb.Status)
		if snapshot.Status.AvailabilityStatus != nil {
			existing.AvailabilityStatus = proto.Clone(snapshot.Status.AvailabilityStatus).(*clustermetadatapb.AvailabilityStatus)
		} else {
			existing.AvailabilityStatus = nil
		}
		if snapshot.Status.ConsensusStatus != nil {
			existing.ConsensusStatus = proto.Clone(snapshot.Status.ConsensusStatus).(*clustermetadatapb.ConsensusStatus)
		} else {
			existing.ConsensusStatus = nil
		}
		if status.PostgresReady {
			existing.LastPostgresReadyTime = now
		}
		// NOTE: when PostgresReady is false, LastPostgresReadyTime is intentionally
		// left at its previous value so callers can reason about "last known good" time.
		existing.StreamSnapshotsReceived++
		return existing
	}

	hs.store.DoUpdate(poolerIDStr, update)

	hs.handleLifecycleSnapshot(ctx, poolerID, snapshot, entry)

	hs.logger.DebugContext(ctx, "health snapshot applied",
		"pooler_id", poolerID,
		"pooler_type", status.PoolerType,
		"postgres_ready", status.PostgresReady,
		"postgres_running", status.PostgresRunning,
	)
}

// handleLifecycleSnapshot arms or cancels the per-pooler shutdown deadline
// timer based on the LifecycleStatus carried in the just-arrived snapshot.
//
// Behaviour:
//
//   - First SHUTTING_DOWN arrival: clamp shutdown_deadline to
//     constants.MinShutdownDeadline (warn-log on clamp), arm
//     time.AfterFunc(deadline, ...) which on expiry synthesizes a local
//     LIFECYCLE_SIGNAL_STOPPED into the cached PoolerHealthState.
//
//   - Duplicate SHUTTING_DOWN arrival: leave the timer alone — the deadline
//     is fixed at first announcement and shouldn't grow. Warn-log only when
//     the announced deadline differs from the stored value (a buggy or
//     misconfigured pooler signal); identical re-announcements are silent.
//
//   - STOPPED arrival: cancel the timer. The cache already shows STOPPED
//     from the snapshot write above, so the analyzer will trigger failover
//     on its next tick.
//
// All other lifecycle values (UNKNOWN, RUNNING, missing) are no-ops here.
func (hs *HealthStream) handleLifecycleSnapshot(ctx context.Context, poolerID string, snapshot *multipoolermanagerdatapb.ManagerHealthSnapshot, entry *streamEntry) {
	reason := "STOPPED snapshot received"
	as := snapshot.GetStatus().GetAvailabilityStatus()
	if as == nil || as.GetLifecycleStatus() == nil {
		return
	}
	lc := as.GetLifecycleStatus()

	switch lc.GetSignal() {
	case clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_SHUTTING_DOWN:
		announced := lc.GetShutdownDeadline().AsDuration()
		clamped := announced
		if clamped < constants.MinShutdownDeadline {
			hs.logger.WarnContext(ctx, "clamping out-of-band shutdown_deadline",
				"pooler_id", poolerID,
				"announced", announced,
				"clamped_to", constants.MinShutdownDeadline)
			clamped = constants.MinShutdownDeadline
		}
		entry.armTimer(ctx, hs, poolerID, clamped)

	case clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED:
		entry.stopTimer()
		hs.synthesizeRequestingDemotion(poolerID, reason)
	}
}

// onShutdownDeadlineExpired runs when the per-pooler shutdown deadline timer
// fires without a STOPPED snapshot or stream EOF having cancelled it. It
// synthesizes a local LIFECYCLE_SIGNAL_STOPPED into the cached
// PoolerHealthState so the analyzer triggers failover on its next tick, and
// cancels the stream context so we stop trying to read from a pooler that has
// missed its own deadline.
//
// Idempotent: if the cache already reflects STOPPED (e.g. a STOPPED snapshot
// raced past us), this is a no-op.
func (hs *HealthStream) onShutdownDeadlineExpired(poolerID string, entry *streamEntry) {
	cached, ok := hs.store.Get(poolerID)
	if !ok {
		return
	}
	if cached.GetAvailabilityStatus().GetLifecycleStatus().GetSignal() == clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED {
		return
	}
	reason := "deadline expired without STOPPED"
	hs.synthesizeShutdownStopped(poolerID, reason)
	hs.synthesizeRequestingDemotion(poolerID, reason)
	entry.cancel()
}

// synthesizeRequestingDemotion writes LeadershipSignal_REQUESTING_DEMOTION into
// the pooler's cached AvailabilityStatus.LeadershipStatus, but only when the
// pooler is the current topology primary (MultiPooler.Type == PRIMARY) and the
// consensus primary_term is > 0. This is what drives LeaderNeedsReplacement to
// fire failover after the orchestrator observes that a primary has stopped.
//
// Called from the three convergent shutdown-intent paths: STOPPED snapshot
// received, stream EOF after SHUTTING_DOWN, and shutdown-deadline timer
// expiry. For non-primary poolers it is a no-op:
// only a leader's departure should request demotion. Idempotent across the
// three paths (skips if the signal is already set for the current term).
//
// Captures the leader_term at observation time so LeaderNeedsReplacement's
// staleness check (leader_term == consensus primary_term) passes when the
// analyzer reads the cached state on its next tick.
func (hs *HealthStream) synthesizeRequestingDemotion(poolerID, reason string) {
	cached, ok := hs.store.Get(poolerID)
	if !ok || cached.MultiPooler == nil {
		return
	}
	if cached.MultiPooler.Type != clustermetadatapb.PoolerType_PRIMARY {
		return
	}
	primaryTerm := commonconsensus.LeaderTerm(cached.GetConsensusStatus())
	if primaryTerm <= 0 {
		return
	}
	as := cached.GetAvailabilityStatus()
	if ls := as.GetLeadershipStatus(); ls != nil &&
		ls.Signal == clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION &&
		ls.LeaderTerm == primaryTerm {
		return
	}

	updateLeadership := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		if existing.AvailabilityStatus == nil {
			existing.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{}
		}
		existing.AvailabilityStatus.LeadershipStatus = &clustermetadatapb.LeadershipStatus{
			Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
			LeaderTerm: primaryTerm,
		}
		return existing
	}
	hs.store.DoUpdate(poolerID, updateLeadership)
	hs.logger.Info("synthesized REQUESTING_DEMOTION locally",
		"pooler_id", poolerID,
		"reason", reason,
		"leader_term", primaryTerm)
}

// synthesizeShutdownStopped writes a local LIFECYCLE_SIGNAL_STOPPED into the
// pooler's cached AvailabilityStatus. Used when the orchestrator has lost
// contact with a pooler that previously announced SHUTTING_DOWN. The
// synthesized STOPPED is observability-only — it makes the cached state
// consistent with what the pooler would have published if its STOPPED
// snapshot had arrived (e.g. for `multigres getpoolers` and EOF-vs-deadline
// tests that inspect the cached lifecycle state). The failover trigger for
// primaries is synthesizeRequestingDemotion, called alongside this helper
// on the same shutdown-intent paths.
//
// This is purely local inference: nothing is sent on the wire.
func (hs *HealthStream) synthesizeShutdownStopped(poolerID, reason string) {
	cb := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		if existing.AvailabilityStatus == nil {
			existing.AvailabilityStatus = &clustermetadatapb.AvailabilityStatus{}
		}
		existing.AvailabilityStatus.LifecycleStatus = &clustermetadatapb.LifecycleStatus{
			Signal: clustermetadatapb.LifecycleSignal_LIFECYCLE_SIGNAL_STOPPED,
		}
		return existing
	}
	hs.store.DoUpdate(poolerID, cb)
	hs.logger.Info("synthesized LIFECYCLE_SIGNAL_STOPPED locally",
		"pooler_id", poolerID,
		"reason", reason)
}

// markConnected records that the stream is connected in the pooler store.
func (hs *HealthStream) markConnected(poolerID string) {
	now := timestamppb.Now()
	cb := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		existing.StreamConnected = true
		existing.StreamConnectedSince = now
		return existing
	}
	hs.store.DoUpdate(poolerID, cb)
}

// markDisconnected records that the stream is disconnected and the pooler
// should be treated as unreachable.
func (hs *HealthStream) markDisconnected(poolerID string) {
	cb := func(existing *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
		existing.IsLastCheckValid = false
		if existing.Status != nil {
			existing.Status.PostgresReady = false
			existing.Status.PostgresRunning = false
		}
		existing.StreamConnected = false
		return existing
	}
	hs.store.DoUpdate(poolerID, cb)
}
