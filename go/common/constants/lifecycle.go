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

package constants

import "time"

// Pooler lifecycle constants shared between the producer (multipooler) and
// consumer (multiorch) of LifecycleStatus signals. Both sides import this
// package to ensure they agree on the floor; an operator-tunable flag would
// invite drift between sides and break the invariant.
const (
	// MinShutdownDeadline is the minimum permissible shutdown_deadline for a
	// LIFECYCLE_SIGNAL_SHUTTING_DOWN announcement.
	//
	// Producer (multipooler): may not announce a deadline below this value.
	// Misconfiguration is treated as a programmer error, not an operator error.
	//
	// Consumer (multiorch): clamps any received deadline to this value and
	// warn-logs when clamping fires (signals a buggy or misconfigured pooler).
	MinShutdownDeadline = 10 * time.Second
)
