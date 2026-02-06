# Indexed Document Benchmark

This benchmark tests the Indexed Document (idoc) feature of Datalevin. It runs
YCSB-style workloads (A/C/F) plus idoc queries to stress nested path lookups,
range predicates, wildcard paths, and array matching.

As a comparison, we also test the document database feature of MongoDB,
PostgreSQL and SQLite under the same workload.

## Benchmark Tasks

Each benchmark run performs the same high-level tasks:

1. **Load**: generate synthetic documents and bulk insert them.
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

#### Idoc Query Mix

The benchmark includes queries that exercise:

- Nested path equality (e.g. profile language)
- Range predicates on numeric fields
- Wildcards `:?` (one segment) and `:*` (any depth)
- Array matching (tags inside events)

### Workloads

- **A**: 50% reads, 50% updates
- **C**: 100% reads
- **F**: 100% read-modify-write

The `--idoc` weight injects idoc queries into the workload. For example, with
`--workload C --idoc 30`, the run will mix point reads with idoc queries where
idoc queries are chosen with weight 30 relative to the base workload.

## Documents

Each document models a small record with these fields:

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
  - Idoc: `(< 0.3 [:stats :score] 0.8)`
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

## Run Benchmarks

```bash
# Datalevin
clj -M:bench --system datalevin

# Postgres (JSONB)
clj -M:bench --system postgres --pg-url jdbc:postgresql://localhost:5432/postgres

# SQLite (JSON1 extension)
clj -M:bench --system sqlite --sqlite-path /tmp/idoc-bench.sqlite

# MongoDB
clj -M:bench --system mongo --mongo-uri mongodb://localhost:27017

# Run all systems sequentially
clj -M:bench --system all
```

### Options

- `--system datalevin|postgres|sqlite|mongo|all`  Run target (default datalevin)
- `--workload A|C|F`  Workload type (default C)
- `--records N`       Number of documents to load (default 10000)
- `--ops N`           Number of operations to run (default 10000)
- `--warmup N`        Warmup ops before measurement (default 1000)
- `--threads N`       Number of worker threads (default 1)
- `--batch N`         Load batch size (default 1000)
- `--idoc N`          Weight for idoc queries (default 30)
- `--hotset P`        Hotset fraction for id selection (0-1, default 1.0)
- `--dir PATH`        Datalevin DB directory (default /tmp/idoc-bench-<uuid>)
- `--dtlv-uri URI`    Datalevin server URI (e.g. dtlv://host:port)
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

## Results

The explain functions of Datalevin/MongoDB/PostgreSQL all report timing, so
these are recorded as results. SQLite does not, so the wall clock time is
recorded.



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
- SQLite limitation: SQLite cannot create indexes on nested array elements.
  Queries like `wildcard-depth` (searching `events[*].entity.name`) and `array`
  (searching `events[*].tags`) cannot use indexes in SQLite and require full
  table scans with `json_each`. This is a fundamental limitation of SQLite's
  JSON1 extension and affects performance for these query types.
