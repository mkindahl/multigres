# Migration demo — command-pane script

Run these in the dashboard's command pane (pane 1). Prereqs: cluster + port-forwards up, source started (`./launch-migration-source.sh`), `bin/multigres` built.

`./demo-dashboard.sh` resets the target side on start (drops any leftover migration and the copied `public.accounts` so a re-run bootstraps cleanly — pass `--no-reset` to skip, `--reset-source` to also reseed the source), starts the write client in the background (transfers only, so the account set stays fixed), and self-heals the balance panes — the target pane (4) shows `waiting for target ledger…` until the migration streams, then fills in on its own, and reconnects itself through the gateway after a target failover; no manual restart needed.

The command pane also defines an `mg` helper — `bin/multigres` with `--admin-server` prefilled — so the commands below use `mg` instead of the full binary and flag.

When running the demo, we need a few addresses and we're going to store them in environment variables.

- `SRC_IP` is the IP address of the source _as seen from inside the container_.
- `SRC_DSN` is the in-cluster address the migration uses (the target's Postgres reaches it over the kind network).
- `SRC_LOCAL_DSN` is the host-reachable port-forward you run `psql` against from your machine. This is typically mapped to a fixed address and we will use 5433 here.

```bash
SRC_IP=$(docker inspect pg-source | jq -r '.[0].NetworkSettings.Networks.kind.IPAddress')
SRC_DSN="host=$SRC_IP port=5432 user=postgres password=sourcepass dbname=postgres sslmode=disable"
SRC_LOCAL_DSN="host=localhost port=5433 user=postgres password=sourcepass dbname=postgres sslmode=disable"
```

1. First step is to create and start the migration. Here we save the result from the multigres command to a file so that we can extract information we need from it after the execution.

```bash
mg create-migration \
      --source-dsn "$SRC_DSN" --target-database postgres --tables public.accounts | \
      tee /tmp/migration.json
ID=$(jq -r .id /tmp/migration.json)
mg start-migration --id "$ID"
```

1. Writes are already running (started by the dashboard).

2. Show DDL replication (the column appears then disappears in panes 3+4). Use `$SRC_LOCAL_DSN` — the host address — not `$SRC_DSN` (the in-cluster one the host can't reach):

```bash
psql "$SRC_LOCAL_DSN" -c 'ALTER TABLE public.accounts ADD COLUMN note text'
psql "$SRC_LOCAL_DSN" -c 'ALTER TABLE public.accounts DROP COLUMN note'
```

4. Kill the target primary mid-migration, then cut the app over to Multigres. Ask multiadmin which pooler is the primary (this follows the primary across failovers) and delete its pod — the coordinator resumes on the newly elected primary:

```bash
PRIMARY_ID=$(mg getpoolers \
  | jq -r '.poolers[] | select(.routing_state.role=="ROUTING_ROLE_PRIMARY") | .id.name')
PRIMARY_POD="multipooler-zone1-$PRIMARY_ID"
kubectl --context kind-multidemo -n default delete pod "$PRIMARY_POD"

kill -USR1 $(pgrep -f 'appclient write')   # cut the app over to Multigres
```

Commands to run manually:

```bash
mg getpoolers | jq -r '.poolers[] | "\(.id.name)\t\(.routing_state.role)"'
kubectl --context kind-multidemo -n default delete pod multipooler-zone1-0

kill -USR1 $(pgrep -f 'appclient write')   # cut the app over to Multigres
```

5. Tear down:

```bash
mg drop-migration --id "$ID"
```

Starting the dashboard already resets the target side (see above), so a fresh run needs no manual step. To reset by hand between runs — drop the migration(s), stop the writer, drop the copied target table, and optionally reseed the source — **while keeping the cluster and infra running**, use the reset script:

```bash
./reset-demo.sh                # full reset (also recreates + reseeds the source)
./reset-demo.sh --keep-source  # reset migration + target table only; leave the source as-is
```

## Inspecting the cluster (pods, primary, topology)

List pods (the kind cluster uses its own kube context):

```bash
kubectl --context kind-multidemo -n default get pods -o wide
```

Find the primary pooler — the one whose postgres is not in recovery:

```bash
for p in 15433:zone1-0 15434:zone1-1 15435:zone1-2; do
  [ "$(PGPASSWORD=postgres psql -h localhost -p ${p%%:*} -U postgres -tAc 'select pg_is_in_recovery()' 2>/dev/null)" = f ] && echo "primary: multipooler-${p##*:}"
done
```

Inspect the topology in etcd. Records are proto-encoded, so decode them with the repo's `protoc`; the primary is the pooler whose `routing_state.role` is `ROUTING_ROLE_PRIMARY`:

```bash
KEX="kubectl --context kind-multidemo -n default exec etcd-0 -- etcdctl"
DECODE="dist/protoc-25.1/bin/protoc --decode=clustermetadata.Multipooler --proto_path=proto proto/clustermetadata.proto"

# whole topology tree (cells, databases, gateways, orchs, shards, poolers)
$KEX get --prefix --keys-only /multigres

# decode each pooler record; PRIMARY = routing_state { role: ROUTING_ROLE_PRIMARY }
for key in $($KEX get --prefix --keys-only /multigres | grep '/poolers/.*/Pooler$'); do
  echo "== $key =="
  $KEX get "$key" --print-value-only | $DECODE | grep -E 'hostname:|type:|role:|rule:'
done
```
