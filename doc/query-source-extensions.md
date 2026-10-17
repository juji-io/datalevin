# Query-source Extensions Appendix

Status: proposed optional contracts and companion extensions  
Release placement: [roadmap source milestones](../roadmap.md#source-milestones)

This appendix collects work beyond the [baseline query-source SPI](query-source-spi.md)
and [companion plan](../../federation/federation.md). Extension negotiation follows
[the SPI rules](query-source-spi.md#version-negotiation-and-source-information).

Bulk bindings has a proposed optional v1 envelope. Probes and the other protocol
extensions below still require envelope design and conformance gates.
DataFusion/Parquet is a companion backend choice, not an SPI feature keyword.

## 1. Bulk bindings

The proposed optional SPI v1 feature keyword is `:bulk-bindings`.
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

### Additional conformance

Run the [F1 execution suite](../roadmap.md#f1-core), adding
batched-key association, multiplicity, snapshot, and request-limit checks.
Verify request count scales with batches, including backend parameter limits,
duplicate-heavy inputs, and skewed joins.

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
Server, native-image, and language-binding modes need their own supported matrix.
External CDC/IVM needs a separate change-data contract under the
[IVM input scope](../IVM.md#23-non-goals-for-the-positive-core).
Distributed shuffle, arbitrary SQL/functions, automatic cross-system identity,
and unrestricted streams remain separate future work.
