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

// Measures the wire size of a representative ManagerHealthSnapshot.
package main

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func mpID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "test-cell",
		Name:      name,
	}
}

func main() {
	cohort := []*clustermetadatapb.ID{mpID("pooler-1"), mpID("pooler-2"), mpID("pooler-3")}

	primarySnap := &multipoolermanagerdatapb.ManagerHealthSnapshot{
		Status: &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
				IsInitialized:    true,
				HasDataDirectory: true,
				PostgresRunning:  true,
				PostgresStatus:   multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PRIMARY,
				WalPosition:      "0/1A2B3C4D",
				ShardId:          "default/0-inf",
				CohortMembers:    cohort,
				PostgresReady:    true,
				PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
					Lsn:                "0/1A2B3C4D",
					Ready:              true,
					ConnectedFollowers: []*clustermetadatapb.ID{mpID("pooler-2"), mpID("pooler-3")},
				},
			},
			AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_ACTIVE,
					LeaderTerm: 5,
				},
				CohortEligibilityStatus: &clustermetadatapb.CohortEligibilityStatus{
					Signal: clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
				},
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id: mpID("pooler-1"),
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						LeaderId:      mpID("pooler-1"),
						CohortMembers: cohort,
						RuleNumber: &clustermetadatapb.RuleNumber{
							CoordinatorTerm: 5,
						},
					},
					Lsn: "0/1A2B3C4D",
				},
			},
		},
		Timeout: durationpb.New(90 * 1_000_000_000),
		Trigger: multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_HEARTBEAT,
	}

	wire1, _ := proto.Marshal(primarySnap)
	fmt.Printf("primary heartbeat snapshot (3-node cohort): %d bytes\n", len(wire1))

	standbySnap := proto.Clone(primarySnap).(*multipoolermanagerdatapb.ManagerHealthSnapshot)
	standbySnap.Status.Status.PoolerType = clustermetadatapb.PoolerType_REPLICA
	standbySnap.Status.Status.PostgresStatus = multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_STANDBY
	standbySnap.Status.Status.PrimaryStatus = nil
	standbySnap.Status.Status.ReplicationStatus = &multipoolermanagerdatapb.StandbyReplicationStatus{
		PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
			Host: "localhost",
			Port: 5432,
		},
		LastReplayLsn:      "0/1A2B3C4D",
		LastReceiveLsn:     "0/1A2B3C4D",
		LastMsgReceiveTime: timestamppb.Now(),
		WalReceiverStatus:  "streaming",
	}
	wire2, _ := proto.Marshal(standbySnap)
	fmt.Printf("standby heartbeat snapshot (3-node cohort): %d bytes\n", len(wire2))

	min := &multipoolermanagerdatapb.ManagerHealthSnapshot{
		Status: &multipoolermanagerdatapb.StatusResponse{
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
			},
		},
		Timeout: durationpb.New(90 * 1_000_000_000),
		Trigger: multipoolermanagerdatapb.SnapshotTrigger_SNAPSHOT_TRIGGER_HEARTBEAT,
	}
	wire3, _ := proto.Marshal(min)
	fmt.Printf("minimal snapshot (uninitialized pooler):    %d bytes\n", len(wire3))
}
