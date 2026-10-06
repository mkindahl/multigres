// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMigrationDirectionIdempotence drives SetMigrationDirection's two
// self-guarding, no-error cases once a migration is EXPORTING: calling it
// again with IMPORT is a valid rollback (not rejected — there is no separate
// IMPORT-only "start" verb anymore), and calling it again with EXPORT (the
// current direction) is a no-op.
func TestMigrationDirectionIdempotence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(
		t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	id := activatedMigration(t, ctx, mt, srcPort, targetDB)

	// Setting EXPORT again on an already-EXPORTING migration is a no-op.
	noopResp, err := mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT})
	require.NoError(t, err, "setting the current direction again must be a no-op, not an error")
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT, noopResp.GetStatus().GetActiveDirection())

	// Setting IMPORT on an EXPORTING migration is the rollback, not rejected —
	// there is no separate IMPORT-only "start" verb to guard against.
	rollbackResp, err := mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT})
	require.NoError(t, err, "setting IMPORT on an EXPORTING migration must roll back, not error")
	require.Equal(t, migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT, rollbackResp.GetStatus().GetActiveDirection())

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: idRef(id), Force: true})
	require.NoError(t, err)
}

// TestUpdateMigrationGuards drives the field-masked update paths that are only
// reachable while a migration is still CREATED — repointing the source DSN (a
// plain row rewrite, before any subscription exists), re-resolving the table
// selection, and changing the sequence margin — plus the rejection of a table
// change once the migration has started.
func TestUpdateMigrationGuards(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Multigres Migrator e2e in short mode")
	}
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL client/server binaries not on PATH")
	}
	ctx := t.Context()

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()
	primary := setup.GetPrimary(t)
	shardsetup.WaitForManagerReady(t, primary.Multipooler)
	targetDB := waitForPrimaryDB(t, ctx, setup)

	srcPort := startStandaloneSource(t)
	seedSource(t, ctx, srcPort)

	mt, mtClose := migrationClient(t, primary)
	defer mtClose()

	connName := createTestConnection(t, ctx, mt, sourceDSN(srcPort))
	createResp, err := mt.CreateMigration(ctx, &migratorpb.CreateMigrationRequest{
		Migration: &migratorpb.Migration{
			Target:         &clustermetadatapb.ShardKey{Database: targetDB},
			ConnectionName: connName,
			Objects:        objs("public.orders"),
		},
	})
	require.NoError(t, err)
	id := createResp.GetMigration().GetId()

	// Repoint the source connection while CREATED: no subscription exists yet, so
	// this is a plain row rewrite (no ALTER SUBSCRIPTION), still validated as the
	// same source database. Same connection name (and thus DSN), re-validated.
	_, err = mt.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
		Migration:  &migratorpb.Migration{Id: id, ConnectionName: connName},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"connection_name"}},
	})
	require.NoError(t, err, "source connection may be repointed while CREATED")

	// Re-resolve the table selection while CREATED: add a second owned table and
	// widen the selection; the update re-validates and stores the concrete list.
	sc := dialSource(t, ctx, srcPort)
	_, err = sc.Query(ctx, "CREATE TABLE public.items (id bigint PRIMARY KEY)")
	require.NoError(t, err)
	_ = sc.Close()
	updResp, err := mt.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
		Migration:  &migratorpb.Migration{Id: id, Objects: objs("public.orders", "public.items")},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"objects"}},
	})
	require.NoError(t, err, "tables may be changed while CREATED")
	require.ElementsMatch(t, []string{"public.orders", "public.items"},
		updResp.GetMigration().GetObjects().GetTable().GetQualifiedNames(), "the widened selection must be re-resolved and stored")

	// Change the sequence margin while CREATED.
	_, err = mt.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
		Migration:  &migratorpb.Migration{Id: id, SequenceMargin: 1000},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"sequence_margin"}},
	})
	require.NoError(t, err, "sequence margin may be changed while CREATED")

	// Start the migration; once it is past CREATED, the table selection is frozen.
	_, err = mt.SetMigrationDirection(ctx, &migratorpb.SetMigrationDirectionRequest{Ref: idRef(id), Direction: migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := mt.GetMigration(ctx, &migratorpb.GetMigrationRequest{Ref: idRef(id)})
		return err == nil && resp.GetStatus().GetCaughtUp()
	}, 60*time.Second, 500*time.Millisecond, "migration must catch up")

	// List all migrations (ListMigrations returns ids; GetMigration fetches each):
	// the streaming migration must appear with its live subscription status merged in.
	listResp, err := mt.ListMigrations(ctx, &migratorpb.ListMigrationsRequest{})
	require.NoError(t, err, "listing all migrations must succeed")
	var found bool
	for _, gotID := range listResp.GetIds() {
		if gotID == id {
			found = true
		}
	}
	require.True(t, found, "the streaming migration must appear in the list-all result")

	_, err = mt.UpdateMigration(ctx, &migratorpb.UpdateMigrationRequest{
		Migration:  &migratorpb.Migration{Id: id, Objects: objs("public.orders")},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"objects"}},
	})
	require.ErrorContains(t, err, "CREATED", "the table selection must be frozen once the migration has started")

	_, err = mt.DropMigration(ctx, &migratorpb.DropMigrationRequest{Ref: idRef(id), Force: true})
	require.NoError(t, err)
}
