# Query-source Extensions Appendix

Status: proposed optional contracts and companion extensions  
Release placement: [roadmap source milestones](../roadmap.md#source-milestones)

This appendix collects work beyond the [baseline query-source SPI](query-source-spi.md)
and [companion plan](../../federation/federation.md). Extension negotiation follows
[the SPI rules](query-source-spi.md#version-negotiation-and-source-information).

The earlier bulk-binding envelope is retained below as input to the
[SPI four-shape revision](query-source-spi.md#11-four-shape-revision-target).
Parameterized access is now required baseline design work before F1 sign-off,
not a follow-up that may wait until after F3. Probes and the other protocol
extensions below still require envelope design and conformance gates.
DataFusion/Parquet is a companion backend choice, not an SPI feature keyword.

<a id="1-bulk-bindings"></a>

## 1. Required input bindings (revision input)

This is the earlier optional proposal, not an implemented or final revised
baseline envelope. Its optional SPI v1 feature keyword was `:bulk-bindings`.
The revision must settle baseline fields and negotiation before compatibility
commitment; the revised baseline should not depend on the earlier optional
feature flag. The earlier proposal specifies:

When negotiated, an alternative may declare nonempty `:requires`, an ordered vector of
typed variable columns. Its cursor-open request also includes
`:bindings [{:binding-id 0 :values [...]} ...]` under explicit row/byte limits.
Core deduplicates typed binding keys and retains their association with every
outer tuple, including multiplicity. IDs are query-local opaque integers.

Returned batches include `:binding-ids`, parallel to `:rows`, referring only to
requested IDs. Empty results for a key require no row. Core fans matches back
to all corresponding outer tuples before Datalog projection/aggregation. Rows
cannot depend on request or response order to identify their key.

Source parameter limits may split one binding batch into bounded requests.
Each request consumes the shared budget and uses the same session view. This
contract does not permit rescanning a one-shot source for each binding batch.
The revised baseline must also handle providers without a bulk endpoint:
splitting into per-key requests consumes actual request/concurrency budgets and
is reflected in estimates and metrics. Lookup-only paths require a finite seed
of typed inputs; missing seeds and unseeded cyclic dependencies fail before execution.
Session consistency and replay are independent: a fresh HTTP call is not stable
replay, and independent one-shot paths need explicit consumption groups.

### Additional conformance

Run the [F1 execution suite](../roadmap.md#f1-core), adding
batched-key association, multiplicity, snapshot, and request-limit checks.
Measure request count under backend parameter limits,
duplicate-heavy inputs, and skewed joins. Add scan-only and lookup-only
providers, native/file/API inputs feeding lookups, absent or unseeded cyclic requirements,
non-bulk endpoints, mixed consistency, one-shot reuse, truncation, and cleanup.
A provider lacking a batch endpoint may scale requests per key within budgets;
only providers advertising a real bulk operation are held to batched request scaling.

## 2. DataFusion and Parquet

Choose an optional JVM/JNI, Rust library/Arrow C interface, or companion-process
boundary after the first workflows pass. Evaluate streaming, cancellation,
buffer lifetime/copying, failure containment, packaging, and dependency
isolation. Reuse DataFusion Federation/Table Providers inside the selected
backend where they pass conformance. Pin the tested engine/provider/Arrow
version set and upgrade it as a unit. Add Parquet discovery/execution and rerun
the [F1 execution suite](../roadmap.md#f1-core).

## 3. Conditioned planning evidence

Design a negotiated probe extension separately. Compile selected counts/samples
from the same execution IR and retained existence predicates. Core allocates
one cancellable native/external work budget and can always choose no probes.
Evidence identifies its population, clause/condition/metadata fingerprints,
bindings, snapshot, cost, and exactness/bounds/confidence. A bounded count is
exact only on exhaustion; an estimated zero never proves emptiness. Exact-zero
shortcuts additionally require the execution read view and complete coverage.

Samples identify method, seed, bias, and inclusion probabilities/weights when
known. A row prefix is a biased observation, not a representative sample.
Sampling before filtering measures a different population; report it as such.
Correlated probes use bounded key batches, not N+1 requests. Samples remain
query-scoped source data and are excluded from logs/schema/shared caches.
Evidence from another snapshot is advisory. Execution reuse requires an
explicit same-view continuation contract; otherwise execution restarts normally.
Implement PostgreSQL probes independently of the optional DataFusion backend.

## 4. Other negotiated capabilities

Shared executable-plan caches, borrowed/columnar batches, source limits/order,
and aggregate delegation each need a separate contract and conformance gate.
Aggregate delegation follows the [shared aggregate contract](../analytics.md#6-shared-aggregate-semantics).
These capabilities apply to any access shape that can implement their semantics.
OLAP is a workload/cost profile, not an SPI source kind; a SQL or resource API
label does not imply aggregate, ordering, or limit support. Capability contracts
must specify supported types, exactness, required inputs, output properties,
and interaction with residual predicates, joins, duplicates/`:with`, and
read consistency before advertising support. In particular, a source-local
limit cannot discard rows required by later residual checks or joins.

Use the [common capability/cost model](query-source-spi.md#12-capability-and-cost-planning-target)
to compare delegated and local execution, including transferred rows/bytes,
provider work, startup, and local retained state. For analytical workloads,
measure reductions from exact aggregation and legal ordering/limit pushdown;
maximum pushdown is not the selection rule. Columnar transfer has its own
ownership and lifetime gate and is independent of relational pushdown support.
Keep these extensions separately gated; naming them here does not enable them
in the baseline or change their roadmap placement.

Server, native-image, and language-binding modes need their own supported matrix.
External CDC/IVM needs a separate change-data contract under the
[IVM input scope](../IVM.md#23-non-goals-for-the-positive-core).
Distributed shuffle, arbitrary SQL/functions, automatic cross-system identity,
and unrestricted streams remain separate future work.
