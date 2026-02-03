# Index Documents Benchmark

This benchmark runs YCSB-style workloads (A/C/F) plus idoc queries to stress
nested path lookups, range predicates, wildcard paths, and array matching.

## Run

```bash
cd benchmarks/idoc-bench
clj -M:bench --system datalevin --workload C --records 100000 --ops 100000 --idoc 30
```

### Other systems

```bash
# Postgres (JSONB)
clj -M:bench --system postgres --pg-url jdbc:postgresql://localhost:5432/postgres

# SQLite (JSON1 extension)
clj -M:bench --system sqlite --sqlite-path /tmp/idoc-bench.sqlite

# MongoDB
clj -M:bench --system mongo --mongo-uri mongodb://localhost:27017

# Run all systems sequentially
clj -M:bench --system all
```

## Options

- `--system datalevin|postgres|sqlite|mongo|all`  Run target (default datalevin)
- `--workload A|C|F`  Workload type (default C)
- `--records N`       Number of documents to load (default 100000)
- `--ops N`           Number of operations to run (default 100000)
- `--warmup N`        Warmup ops before measurement (default 2000)
- `--threads N`       Number of worker threads (default 1)
- `--batch N`         Load batch size (default 1000)
- `--idoc N`          Weight for idoc queries (default 20)
- `--hotset P`        Hotset fraction for id selection (0-1, default 1.0)
- `--dir PATH`        Datalevin DB directory (default /tmp/idoc-bench-<uuid>)
- `--sqlite-path PATH` SQLite DB file path
- `--pg-url URL`      Postgres JDBC URL
- `--pg-user USER`    Postgres user
- `--pg-password PWD` Postgres password
- `--pg-table NAME`   Postgres table name (default idoc_bench_docs)
- `--mongo-uri URI`   MongoDB URI
- `--mongo-db NAME`   MongoDB database (default idoc_bench)
- `--mongo-coll NAME` MongoDB collection (default docs)
- `--seed N`          RNG seed (default 42)
- `--keep`            Keep DB artifacts after run

## Workloads

- **A**: 50% reads, 50% updates
- **C**: 100% reads
- **F**: 100% read-modify-write

The `--idoc` weight injects idoc queries into the workload. For example, with
`--workload C --idoc 30`, the run will mix point reads with idoc queries where
idoc queries are chosen with weight 30 relative to the base workload.

## Bench Tasks

Each run performs the same high-level tasks:

1. **Load**: generate synthetic agent-memory docs and bulk insert them.
2. **Index/Analyze**:
   - Datalevin runs `analyze` to refresh sampling stats.
   - Postgres/SQLite build expression indexes for the query mix and run `ANALYZE`.
   - MongoDB builds field indexes for the query mix.
3. **Warmup**: execute `--warmup` operations to stabilize caches.
4. **Run**: execute `--ops` operations according to the workload mix.

### Operation Types

- **read**: point read by document id.
- **update**: update an existing document with new `:stats` values.
- **rmw**: read-modify-write on `:stats` values.
- **idoc**: execute one of the idoc query shapes (nested, range, wildcards, array).

## Idoc Query Mix

The benchmark includes queries that exercise:

- Nested path equality (e.g. profile language)
- Range predicates on numeric fields
- Wildcards `:?` (one segment) and `:*` (any depth)
- Array matching (tags inside events)

These are representative of realistic context-based retrieval.

## Agent-Memory Documents

Each document models a small “agent memory” record with these fields:

- `:profile` — `:age`, `:lang`, `:persona`
- `:stats` — `:score` (float), `:last_seen` (timestamp millis)
- `:facts` — string-keyed map with `"city"` and `"team"`
- `:memory` — `:topic`, `:source`, `:entities`
- `:tags` — vector of tag strings
- `:events` — vector of event maps, each with:
  - `:ts` (timestamp millis), `:kind`, `:tags`, `:entity {:name ...}`, `:score`

Example (abbreviated):

```clojure
{:profile {:age 34 :lang "en" :persona "developer"}
 :stats   {:score 0.62 :last_seen 1700001234567}
 :facts   {"city" "SF" "team" "blue"}
 :memory  {:topic "roadmap" :source "chat" :entities ["acme" "globex"]}
 :tags    ["urgent" "note"]
 :events  [{:ts 1700002345678
            :kind "chat"
            :tags ["feature" "todo"]
            :entity {:name "acme"}
            :score 0.41}]}
```

## Query Shapes

The `idoc` operation randomly picks one of these query shapes:

- **Nested equality**: match a profile language.
  - Idoc: `{:profile {:lang "en"}}`
  - SQL/Mongo: `profile.lang = 'en'`
- **Range predicate**: score between two values.
  - Idoc: `(between [:stats :score] 0.3 0.8)`
  - SQL/Mongo: `stats.score BETWEEN 0.3 AND 0.8`
- **Wildcard (one segment)**: match any `:facts` value.
  - Idoc: `{:facts {:? "SF"}}`
  - SQL/Mongo: match `"facts.city" = "SF"` or `"facts.team" = "SF"`
- **Wildcard (any depth)**: match nested events by entity name.
  - Idoc: `{:* {:entity {:name "acme"}}}`
  - SQL/Mongo: match any `events[].entity.name = "acme"`
- **Array match**: match tags inside events.
  - Idoc: `{:events {:tags "urgent"}}`
  - SQL/Mongo: match any `events[].tags` containing `"urgent"`

## Notes

- Postgres uses JSONB, SQLite uses JSON1 functions, and MongoDB stores the
  document fields at the top level with `_id` as the record id.
- Datalevin runs `analyze` after loading to refresh sampling statistics.
- Postgres/SQLite build expression indexes for the query mix and run ANALYZE
  after loading. MongoDB builds field indexes for the same queries.
- Wildcard-depth and array predicates in SQL and Mongo are implemented against
  the `events` array to match the generated documents.
- The benchmark drops Postgres tables / Mongo collections and removes SQLite
  files unless `--keep` is supplied.
- A Homebrew-based MongoDB install helper lives at
  `benchmarks/idoc-bench/scripts/install-mongodb.sh`.
