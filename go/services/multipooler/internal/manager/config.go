// Copyright 2025 Supabase, Inc.
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

// Package manager implements the core MultipoolerManager business logic
package manager

import (
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
)

// Config holds configuration for the MultipoolerManager
type Config struct {
	SocketFilePath             string
	TopoClient                 topoclient.Store
	HeartbeatIntervalMs        int
	PgctldAddr                 string                  // Address of pgctld gRPC service
	ConsensusEnabled           bool                    // Whether consensus gRPC service is enabled
	ConnPoolConfig             *connpoolmanager.Config // Connection pool config (manager created in MultipoolerManager)
	BackendVpidTrackingEnabled bool                    // Whether to write active gateway-vpid/backend-pid mappings

	// PromotionTimeout bounds how long waitForPromotionComplete waits for a
	// freshly promoted postgres to leave recovery and accept connections.
	// Zero means use the default (defaultPromotionTimeout). Exposed as a flag
	// so tests can shrink it well below a real promotion's duration to
	// deterministically exercise the promotion-timeout path.
	PromotionTimeout time.Duration

	// pgBackRest TLS certificate paths for connecting to primary's pgBackRest server
	PgBackRestCertFile string // TLS client certificate file path
	PgBackRestKeyFile  string // TLS client key file path
	PgBackRestCAFile   string // TLS CA certificate file path
}
