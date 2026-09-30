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

// Package grpcmigrationservice serves the Multigres Migrator migration RPCs on the
// multipooler. The handlers are primary-gated (via MigrationCoordinatorIfPrimary)
// and delegate to the co-located migration coordinator; multiadmin forwards
// operator commands here after resolving the shard's current primary.
package grpcmigrationservice

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

type migrationService struct {
	migratorpb.UnimplementedMigratorServer
	manager *manager.MultipoolerManager
}

// RegisterMigrationServices wires the migration gRPC service into the pooler's
// gRPC server and opts the manager into the migration coordinator (reconcile
// poller) when the "migration" service is enabled in the service map.
func RegisterMigrationServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultipoolerManager) {
		if grpc.CheckServiceMap("migration", senv) {
			pm.StartMigrationCoordinator()
			migratorpb.RegisterMigratorServer(grpc.Server, &migrationService{manager: pm})
		}
	})
}

func (s *migrationService) CreateMigration(ctx context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.CreateMigration(ctx, migration.CreateParams{
		SourceDSN:      req.GetMigration().GetSourceDsn(),
		TargetDatabase: req.GetMigration().GetTargetDatabase(),
		TargetShard:    req.GetMigration().GetTargetShard(),
		Name:           req.GetMigration().GetName(),
		Tables:         foldTableSelection(req.GetMigration().GetObjects()),
		CopyData:       !req.SkipCopyData,
		SkipSchemaCopy: req.SkipSchemaCopy,
		SequenceMargin: req.SequenceMargin,
		QuiesceRoles:   req.QuiesceRoles,
	})
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.CreateMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

// foldTableSelection folds the structured selection (a table list, a schema
// list, or all tables owned by the source role) into the flat pattern list the
// coordinator resolves: "*" (all owned tables), "schema.*" (all owned tables in
// a schema), or "schema.table". A nil objects, or an "all" arm explicitly set to
// false, yields no patterns — CreateMigration then rejects the request for
// selecting no tables.
func foldTableSelection(objects *migratorpb.SelectionObject) []string {
	if objects == nil {
		return nil
	}
	switch o := objects.GetObject().(type) {
	case *migratorpb.SelectionObject_All:
		if !o.All {
			return nil
		}
		return []string{"*"}
	case *migratorpb.SelectionObject_Schema:
		patterns := make([]string, 0, len(o.Schema.GetSchemata()))
		for _, s := range o.Schema.GetSchemata() {
			patterns = append(patterns, s+".*")
		}
		return patterns
	case *migratorpb.SelectionObject_Table:
		return o.Table.GetQualifiedNames()
	default:
		return nil
	}
}

// migrationRef picks the addressing key from a request: the explicit id when
// set, else the name. The coordinator resolves id first, then a unique name.
func migrationRef(id int64, name string) migration.Ref {
	return migration.Ref{ID: id, Name: name}
}

func (s *migrationService) StartMigration(ctx context.Context, req *migratorpb.StartMigrationRequest) (*migratorpb.StartMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.StartMigration(ctx, migrationRef(req.Id, req.Name))
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.StartMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

func (s *migrationService) GetMigrations(ctx context.Context, req *migratorpb.GetMigrationsRequest) (*migratorpb.GetMigrationsResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	if ref := migrationRef(req.Id, req.Name); ref.ID != 0 || ref.Name != "" {
		proj, err := coord.GetMigration(ctx, ref)
		if err != nil {
			return nil, toGRPC(err)
		}
		return &migratorpb.GetMigrationsResponse{Migrations: []*migratorpb.MigrationInfo{infoToProto(proj)}}, nil
	}
	projs, err := coord.ListMigrations(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	out := make([]*migratorpb.MigrationInfo, len(projs))
	for i, p := range projs {
		out[i] = infoToProto(p)
	}
	return &migratorpb.GetMigrationsResponse{Migrations: out}, nil
}

func (s *migrationService) GetMigrationJournal(ctx context.Context, req *migratorpb.GetMigrationJournalRequest) (*migratorpb.GetMigrationJournalResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	entries, err := coord.GetMigrationJournal(ctx, migrationRef(req.Id, req.Name))
	if err != nil {
		return nil, toGRPC(err)
	}
	out := make([]*migratorpb.MigrationJournalEntry, len(entries))
	for i, e := range entries {
		out[i] = journalEntryToProto(e)
	}
	return &migratorpb.GetMigrationJournalResponse{Entries: out}, nil
}

func (s *migrationService) DropMigration(ctx context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.DropMigration(ctx, migrationRef(req.Id, req.Name), migration.DropOptions{
		Wait:        req.Wait,
		WaitTimeout: time.Duration(req.WaitTimeoutSeconds) * time.Second,
		Force:       req.Force,
	})
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.DropMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

func (s *migrationService) UpdateMigration(ctx context.Context, req *migratorpb.UpdateMigrationRequest) (*migratorpb.UpdateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	if req.UpdateMask == nil || len(req.UpdateMask.Paths) == 0 {
		return nil, status.Error(codes.InvalidArgument, "update_mask is required")
	}
	var p migration.UpdateParams
	for _, path := range req.UpdateMask.Paths {
		switch path {
		case "source_dsn":
			v := req.SourceDsn
			p.SourceDSN = &v
		case "sequence_margin":
			v := req.SequenceMargin
			p.SequenceMargin = &v
		case "tables":
			v := req.Tables
			p.Tables = &v
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported update_mask path %q", path)
		}
	}
	proj, err := coord.UpdateMigration(ctx, migrationRef(req.Id, req.Name), p)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.UpdateMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

func (s *migrationService) ActivateMigration(ctx context.Context, req *migratorpb.ActivateMigrationRequest) (*migratorpb.ActivateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	opts := migration.ActivateOptions{
		MaxLagBytes: req.MaxLagBytes,
		WaitTimeout: time.Duration(req.WaitTimeoutSeconds) * time.Second,
	}
	proj, err := coord.Activate(ctx, migrationRef(req.Id, req.Name), opts)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.ActivateMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

func (s *migrationService) DeactivateMigration(ctx context.Context, req *migratorpb.DeactivateMigrationRequest) (*migratorpb.DeactivateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.Deactivate(ctx, migrationRef(req.Id, req.Name))
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.DeactivateMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

// toGRPC maps coordinator errors to gRPC status errors.
func toGRPC(err error) error {
	if errors.Is(err, migration.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, migration.ErrNotReady) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return mterrors.ToGRPC(err)
}

// migToProto maps a projection to its static Migration configuration. Objects
// reports the concrete tables resolved at create time (wildcards are already
// expanded), regardless of whether the request selected them by table list,
// schema list, or "all".
func migToProto(p *migration.Projection) *migratorpb.Migration {
	return &migratorpb.Migration{
		Id:             p.ID,
		Name:           p.Name,
		SourceDsn:      p.SourceDSN,
		TargetDatabase: p.TargetDatabase,
		TargetShard:    p.TargetShard,
		Objects:        tablesToSelectionObject(p.Tables),
	}
}

func tablesToSelectionObject(tables []string) *migratorpb.SelectionObject {
	if len(tables) == 0 {
		return nil
	}
	return &migratorpb.SelectionObject{
		Object: &migratorpb.SelectionObject_Table{
			Table: &migratorpb.TableSpec{QualifiedNames: tables},
		},
	}
}

// statusToProto maps a projection to its live MigrationStatus.
func statusToProto(p *migration.Projection) *migratorpb.MigrationStatus {
	s := &migratorpb.MigrationStatus{
		Id:               p.ID,
		Phase:            phaseToProto(p.Phase),
		LastError:        p.LastError,
		TotalRelations:   p.TotalRelations,
		ReadyRelations:   p.ReadyRelations,
		CaughtUp:         p.CaughtUp,
		PublicationName:  p.PublicationName,
		SubscriptionName: p.SubscriptionName,
		CreatedAt:        timestamppb.New(p.CreatedAt),
		ActiveDirection:  dirToProto(p.ActiveDirection),
		LagBytes:         p.LagBytes,
		LagSeconds:       p.LagSeconds,
	}
	if p.StreamingSince != nil {
		s.StreamingSince = timestamppb.New(*p.StreamingSince)
	}
	return s
}

// infoToProto pairs a projection's static configuration with its live status,
// for the RPCs that return more than one migration at a time.
func infoToProto(p *migration.Projection) *migratorpb.MigrationInfo {
	return &migratorpb.MigrationInfo{
		Migration: migToProto(p),
		Status:    statusToProto(p),
	}
}

// journalEntryToProto maps a coordinator journal entry to its proto form. The
// journal never contains credentials, so all fields pass through unredacted.
func journalEntryToProto(e *migration.JournalEntry) *migratorpb.MigrationJournalEntry {
	return &migratorpb.MigrationJournalEntry{
		Seq:           e.Seq,
		MigrationId:   e.MigrationID,
		MigrationName: e.MigrationName,
		Event:         string(e.Event),
		Phase:         phaseToProto(e.Phase),
		Direction:     dirToProto(e.Direction),
		FromLsn:       e.FromLSN,
		ToLsn:         e.ToLSN,
		LastError:     e.LastError,
		Detail:        e.Detail,
		CreatedAt:     timestamppb.New(e.CreatedAt),
	}
}

func phaseToProto(p migration.Phase) migratorpb.MigrationPhase {
	switch p {
	case migration.PhaseCreated:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_CREATED
	case migration.PhaseValidating:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_VALIDATING
	case migration.PhaseSchemaCopy:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_SCHEMA_COPY
	case migration.PhaseCreatePublication:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_CREATE_PUBLICATION
	case migration.PhaseCopying:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_COPYING
	case migration.PhaseImporting:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_IMPORTING
	case migration.PhaseExporting:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_EXPORTING
	case migration.PhaseSwitchingToImport:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_SWITCHING_TO_IMPORT
	case migration.PhaseSwitchingToExport:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_SWITCHING_TO_EXPORT
	case migration.PhaseCompleting:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_COMPLETING
	case migration.PhaseFailed:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_FAILED
	default:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_UNSPECIFIED
	}
}

func dirToProto(d migration.Direction) migratorpb.MigrationDirection {
	switch d {
	case migration.DirectionImport:
		return migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT
	case migration.DirectionExport:
		return migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT
	default:
		return migratorpb.MigrationDirection_MIGRATION_DIRECTION_UNSPECIFIED
	}
}
