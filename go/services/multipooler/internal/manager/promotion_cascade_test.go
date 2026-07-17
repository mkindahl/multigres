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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// TestWaitForPromotionComplete_DoesNotAbandonBeforeReady is the unit-level
// reproduction of the MUL-1004 cascade re-election.
//
// The EKS build (c675c93e) aborts waitForPromotionComplete on its own internal
// deadline even when the caller's context still has plenty of time and postgres
// is legitimately still finishing promotion (WAL replay + accepting
// connections). That premature DEADLINE_EXCEEDED makes promote() clear
// promotionInProgress, POSTGRES_STATUS_PROMOTING drops before the node is
// ready, and multiorch fires a second (LeaderIsDead) election — the cascade
// that ends in two write-capable primaries.
//
// We reproduce it deterministically in ~600ms by shrinking the promotion
// timeout to 200ms (via the new Config.PromotionTimeout seam) and having
// postgres report "not ready" until ~600ms. The caller's context is generous
// (5s), so a correct implementation must keep polling and return success once
// postgres is ready. The buggy build returns DEADLINE_EXCEEDED at 200ms.
//
// EXPECTED: fails on c675c93e (returns an error), passes on a build that
// respects the caller's context instead of an internal deadline.
func TestWaitForPromotionComplete_DoesNotAbandonBeforeReady(t *testing.T) {
	const readyAfterNCalls = 6 // pg_isready is false for the first ~500-600ms

	var statusCalls atomic.Int32
	mockQS := mock.NewQueryService()
	// Out of recovery immediately: promotion's WAL-replay phase is done, so the
	// wait is gated purely on postgres accepting connections (pg_isready).
	mockQS.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))

	pm, mockPgctld := setupManagerWithMockDBAndPgctld(t, mockQS, &fakeRuleStore{})

	// postgres becomes ready only after several polls — a normal promotion that
	// takes longer than the (shrunk) internal deadline.
	mockPgctld.StatusFunc = func(_ *pgctldpb.StatusRequest) (*pgctldpb.StatusResponse, error) {
		n := int(statusCalls.Add(1))
		return &pgctldpb.StatusResponse{
			Status: pgctldpb.ServerStatus_RUNNING,
			Ready:  n >= readyAfterNCalls,
		}, nil
	}

	// Shrink the promotion deadline well below the simulated promotion duration.
	pm.config.PromotionTimeout = 200 * time.Millisecond

	// The caller's context has ample time; a correct promotion wait honors it.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err := pm.waitForPromotionComplete(ctx)
	require.NoError(t, err,
		"waitForPromotionComplete abandoned promotion on its internal deadline while the "+
			"caller's context still had time and postgres was still coming up — this is the "+
			"premature timeout that clears PROMOTING and triggers the MUL-1004 cascade re-election")
}
