# Query-source SPI

Status: earlier F0/F1 baseline implemented; SPI v2 revision specified, implementation pending

Release placement: [roadmap source milestones](../roadmap.md#source-milestones); embedded JVM execution  
Public namespace: `datalevin.query.source`  
Optional contracts: [extensions appendix](query-source-extensions.md)  
Related plans: [companion federation design](../../federation/federation.md),
[analytics](../analytics.md)

This document specifies the boundary between Datalevin's query engine and a
separately developed source implementation. It makes the minimum contracts
concrete for the current external execution path. The
[four-shape revision target](#11-four-shape-revision-target) extends the common
model to native Datalevin, SQL, files/exports, and APIs before commitment.
[Section 10](#10-spi-v2-revised-four-shape-contract) specifies the revised v2
envelopes and negotiation; §§2–9 retain the earlier executable v1 baseline.
Changes remain reviewable during implementation; the revised SPI becomes an
experimental compatibility commitment only after the F1 conformance gates pass.

This is the authoritative core source contract. The companion plan specifies
provider behavior and delivery; it cannot add baseline capabilities here.
The separate [schema definitions contract](schema-definitions.md) specifies
metadata entries, schema snapshots, conditional updates, and fingerprint
vocabulary. Virtual-relation semantics remain in the
[analytics plan](../analytics.md#7-virtual-and-maintained-relations).

## 1. Project boundary

Datalevin proper owns:

- the public SPI and validation of its envelopes;
- Datalog semantics, source-clause collection, plan selection, native joins,
  residual evaluation, and virtual-definition expansion;
- query-owned sessions, cancellation, incremental consumption, resource
  accounting, and cleanup;
- external-query result-cache bypass and redacted explanation; and
- the independent [schema definitions contract](schema-definitions.md).

The sibling directory `../federation` hosts `datalevin-federation`, a separate
project with its own build, dependencies, tests, and releases. It owns every
production external source adapter, including the first application-owned local
relation adapter, APIs, files/exports, PostgreSQL, CSV/dataset integrations, and
optional Parquet/DataFusion. The companion also owns discovery,
mapping syntax and compilation, external entity-reference values,
SQL generation, connections, credentials, and provider-specific conversions.
Native Datalevin remains a built-in core source.

The dependency direction is `datalevin-federation -> datalevin`. Core neither
loads the companion automatically nor depends on its namespaces, drivers,
Arrow, or engine libraries. Core tests use a deterministic fake source in test
code; that fake is not a production adapter or a second federation project.
Provider integrations and their eager-materialization oracles live in the
separate project. Broader integration matrices may also use `../dtlvtest`.

The SPI is an in-process trusted extension boundary, not a provider sandbox or
a wire protocol. Its first implementation supports embedded JVM queries.
Remote server execution of adapters, transport of live sources, native-image
execution, and language-binding entry points are outside v1 scope and require
separate conformance gates. Embedded `d/q` and `d/explain` reject a live source
combined with a remote DB before query dispatch or provider acquisition; input
order does not select a different execution mode. This client-side guard is not
a server-side adapter or serialization contract. No external write or
distributed transaction API is introduced.

Only an explicit `:in` source binding implementing `IQuerySource` enters this
path. Existing Datalevin and collection sources keep their current contracts;
they are not automatically wrapped or reinterpreted as companion adapters.

### 1.1 Four-shape revision target

This section records planned changes before the experimental compatibility
commitment. Sections 2–9 describe the earlier executable external baseline,
which requires unparameterized external alternatives, uses one query-wide
consistency request, and restricts one-shot replay at session scope. These
rules have not yet been replaced in code. Exact new envelopes and negotiation
rules are specified in [Section 10](#10-spi-v2-revised-four-shape-contract) as
SPI v2 and must be implemented and tested before they become usable.

The federation interface is mapped entities and attributes, queried through
source-qualified EAV patterns. Native Datalevin supplies its existing entities
and attributes; companion adapters map external identity, fields, references,
and missing-value semantics into that interface. Core receives the accepted
attribute types, cardinality, entity/reference domains, and absence semantics
through the [v2 logical schema](#logical-schema-guarantees). Physical columns,
endpoint paths, conversion implementations, and supporting proofs remain
provider-owned. Each mapped attribute denotes a typed entity/value relation
with executable access paths. Keep physical
source identity and access scope, accepted mapping metadata, alternative access
paths, and the query-owned read context distinct. Accepted scope and conversion
semantics participate in the definition fingerprint; query input values remain
execution data.

Arbitrary tuple and collection inputs remain existing Datalog facilities.
Planning alternatives and cursor batches use tuples of variable bindings as
their execution representation, including wide results of fused EAV patterns.
Neither introduces a separate tuple-source federation model. V2 requires an
EAV pattern contract as specified in Section 10.1; the tuple source contract
in Sections 2–9 documents the earlier executable v1 baseline.
Providers describe required inputs, returned columns, exact clause coverage,
safe superset filters, and the cost and execution properties of each path.

| Shape | Access-path examples | Provider-specific implementation |
| --- | --- | --- |
| Native Datalevin | Indexed EAV lookup, range access, scan, native fragment | Existing native planner, indexes, read context, and full native query semantics. |
| SQL | Bound lookup, scan, filter/project/join fragment | Dialect translation, driver, transaction, and buffers. |
| Files/exports | Sequential read, supported partition/column pruning | Format reader, object version/manifest, decoding, and one-shot input ownership. |
| API | ID lookup, scoped listing, supported search or batch lookup | Endpoint/query translation, credentials, pagination, rate limits, and retries. |

No provider must support every access kind. Scans are optional; lookup-only
sources are valid. Pushdown capabilities are demonstrated by alternatives,
not inferred from a shape or advertised as mandatory remote relational work.
Physical discovery is optional companion I/O; declared schemas are valid.

Required implementation and conformance work under Section 10:

1. **Typed input dependencies and binding batches.** Promote parameterized
   access from the deferred bulk-binding proposal into the common baseline.
   Providers may still return only input-free alternatives. Core orders
   dependencies, rejects unsatisfied inputs and unseeded dependency cycles,
   deduplicates typed input keys without losing outer multiplicity, and
   associates returned rows with input IDs. Implement bounded cursor-open
   binding batches and request splitting;
   no provider batch endpoint is assumed. Unknown costs remain unknown, and
   per-key HTTP work is explicit and charged rather than hidden as one batch.
2. **Independent read guarantees.** Negotiate requirements per source and
   report actual consistency without silently weakening another source.
   Retain snapshot, statement, and best-effort distinctions; stable replay and
   exact pinned data versions are separate guarantees. Reissuing an API
   request is not identical replay. Implement consumption groups for independent
   one-shot paths sharing a session, and reject reuse or spool under quotas.
   Acquisition may establish an authorized weak context without claiming a
   stable provider snapshot. Memoized observations do not create one.
3. **Scope and completion.** Accepted relation metadata fixes tenant,
   repository, partition, or other configured scope. For supplied bindings,
   successful exhaustion means completion of that accepted access under the
   reported consistency, not a global point-in-time snapshot. Pagination and
   continuation are provider-owned. Known result caps, omitted pages, and
   request-budget exhaustion fail ordinary Datalog execution; they cannot be
   reported as successful exhaustion. A capped/search-defined view may be a
   separately declared relation, never a silently incomplete base relation.
   Superset filters must preserve every matching row; residual checks cannot
   repair missing rows. Weak reads disable shortcuts requiring stable replay.
4. **Native participation.** The existing native planner and executor are
   the specialized implementation of the common relation/access contract.
   Activate the native planner bridge when composing mixed queries with
   external sources. Native-only queries retain their direct execution path,
   full syntax, indexes, caches, prepared paths, and representations, without
   federation planning or allocation of federation runtime state. Section 1.3 defines isolation
   and the mandatory native performance gate.
5. **Four-shape conformance.** Before F1 closes, compare scan and dependent
   access with eager evaluation across native, SQL, file/export, and API
   fixtures. Include empty/duplicate bindings, batch association and request
   splitting, absent required inputs, mixed consistency, independent one-shot
   paths, API truncation/errors, and cross-shape joins. Use real native
   execution and deterministic test providers; fixtures do not certify a real
   SQL driver or API integration. Existing local and fake suites remain gates.
6. **Capability and cost planning.** Implement the admissibility rules and
   conditioned, comparable estimates in Section 1.2. Implement bounded
   federation plan enumeration and cost-based selection across access shapes
   and within one shape for queries with external source dependencies.
   Validate ranking separately from result semantics and hard limits;
   provider or workload labels cannot substitute for capability/cost evidence.

Logical types/equality, exact coverage and residual rules, metadata identity,
authorization, bounded results/work, cancellation, cleanup, and redacted
explanation remain shared. Request/concurrency limits and metrics already
exist in the baseline; reuse their ledger for API pages, retries, and splitting.
SQL ASTs, file formats, endpoint schemas, and discovery stay outside core.
Ordering, aggregate delegation, probes, and borrowed batches remain separate
capabilities. Production API and export delivery precedes PostgreSQL; release
placement and both owners' acceptance gates remain in the
[roadmap](../roadmap.md#source-milestones).

### 1.2 Capability and cost planning target

This is part of the planned baseline revision. The existing estimate fields in
Section 5 remain the executable envelope until the revised units, conditioning,
and planning inputs in [Section 10.6](#106-conditioned-planning-and-deterministic-selection)
are implemented and tested. Federation
selection uses this model for external and mixed queries. Native fragments
participate through the existing optimizer; native-only queries retain native
optimization without a second generic planning pass.

Native, SQL, file/export, and resource-API access describe interfaces. OLTP,
OLAP, and hybrid are workload profiles within those interfaces, not additional
source kinds. SQL over HTTP retains SQL semantics; HTTP transport does not make
it a resource lookup/list API. One system may expose several interfaces.
Workload labels may describe a deployment, but grant no operation, consistency
guarantee, or cost preference.

**Capabilities determine admissibility.** An alternative must be valid for the
requested operator, logical input types, bindings, accepted scope, and read
context. Exact coverage and safe superset prefilters retain their distinct
semantics. Required inputs, output columns, replay/consumption, completion, and
hard provider limits are validated before ranking. Negotiating a protocol
feature establishes envelope support; it does not prove exact support for a
particular operation. Discovery capability lists remain advisory.

Lookup, scan, pruning, filter/projection, and same-context joins may be offered
independently. Aggregate/ordering/limit delegation and borrowed/columnar
transfer remain separately specified capabilities with conformance gates. Any source
may implement them; no OLAP source kind or mandatory OLAP capability bundle is
introduced. Core may retain an operation locally when the provider lacks its
contract and complete bounded input can be obtained.

**Costs rank complete admissible plans.** Providers report estimates in common
units for a physical alternative invocation at an explicit input/batch size.
Core supplies bounded planning information about known constants, accepted
scope, and estimated input rows/distinct keys, preserving unknowns. Estimates
identify their conditioning, confidence, provenance, and statistics freshness;
they must not assume that a lookup's input count is one or that a scan is free.
Core derives invocation counts and whole-plan cost; session setup is separate
one-time work. Section 10.6 fixes field names, units, and inclusion rules.

| Dimension | Required meaning when an estimate is supplied |
| --- | --- |
| Output cardinality and width | Expected returned rows and logical bytes per row for the stated inputs; retained-value charges and wire bytes remain distinct. |
| Physical access work | Rows/bytes/partitions examined where known, separately from returned rows; unknown backend work stays unknown. |
| Requests | Expected operations/pages for the stated batch, including provider parameter splitting; provider batch/concurrency/rate ceilings are separate constraints. |
| Time | Startup and incremental execution/request latency in stated time units, with explicit inclusion and overlap rules to avoid double counting. |
| Transfer | Expected bytes crossing the source boundary, accounting for projection and any valid reduction at the source. |
| State | Estimated peak retained memory and spill work for blocking execution or replay; these are estimates, not reservations or guaranteed bounds. |

Core evaluates bounded combinations of alternatives with join order, input
deduplication/fan-out, decoding, residual checks, local blocking operators,
spools, and required transfers. Count fused physical work once and shared work
once only when reuse is legal; include each dependent invocation. A smaller
fragment or local operation can be cheaper than maximum pushdown. Requested
concurrency is constrained by provider and query limits; rate-limit waiting
and request latency affect expected elapsed time.

The initial ranking policy targets estimated end-to-end latency subject to
admissibility and hard limits. Use Section 10.6's deterministic structural
fallback when evidence cannot support a comparison, retain uncertainty in
explain, and calibrate the policy against measured workloads. Provider-local optimizer
scores or billing units cannot be compared or added across systems without an
explicit conversion policy. Unknown components remain unknown, never zero.
Estimates do not prove emptiness, uniqueness, complete results, or resource
feasibility. Runtime accounting enforces limits despite inaccurate estimates.

Baseline planning uses accepted metadata and bounded planning inputs without
source I/O. Live counts, samples, and source EXPLAIN remain the separately
budgeted probe extension. Statistics freshness affects cost confidence, not
the logical definition fingerprint or a query's read-view guarantees.

Conformance must show that a cheap invalid alternative is rejected, identical
capabilities/costs produce the same selection after an OLTP/OLAP relabeling,
and controlled changes in selectivity, distinct keys, latency, transfer, or
batch limits can change the preferred valid plan. Cover unknown, stale, and
deliberately wrong estimates without changing correct results or quota
enforcement. Explain separates capability rejection from cost ranking and
reports estimate assumptions/confidence separately from actual work, using
bounded structural diagnostics that do not expose bindings or provider secrets.

### 1.3 Native execution isolation and performance

Native Datalevin is a specialized implementation of the shared semantics and
access contract. The contract does not require a common general-purpose
executor or public protocol calls inside native hot loops. Native-only queries,
including existing collection inputs and native-only rules/virtual expansions,
keep the native optimizer, executor, index operations, caches, prepared-query
specializations, and internal value representations.

Select the execution route at the query or compiled-fragment boundary.
Native-only work must not construct federation descriptors or estimate maps,
validate external envelopes, enumerate source alternatives, open extension
sessions/cursors, initialize federation ledgers, or copy rows into external
owned-batch representations. Shared correctness rules may reuse existing
specialized native code; they do not require new per-row/per-datom protocol
dispatch or accounting on the native-only path.

The entry-point guard gives native DB values precedence over `IQuerySource`,
including protocol extensions to the DB class. Adapters load the public
`datalevin.query.source` namespace when implementing its protocols; this installs
source detection without loading the executor. Core-only applications leave
that optional detection inactive. Direct implementations and late protocol
extensions are both supported. The executor resolves on the first external
invocation; native-only calls retain their route before and after that load.
The mixed planner uses the same native precedence. No adapter registration API
or per-row dispatch is added. Retain routing regression checks alongside the
existing supported-query and cache behavior checks.

For mixed queries, expose native fragment capabilities and cost at planning
boundaries and invoke native operations directly within each selected fragment.
Federation coordination, conversions, and required resource accounting belong
to that mixed-query execution and its boundaries. This does not exempt mixed
native work from the shared limits or promise zero coordination overhead.

Native-only performance is a release constraint. Any reproducible slowdown or
additional allocation attributable to federation blocks integration until
eliminated; faster external queries do not offset it. Benchmark the existing
input guards, prepared paths, cache hits, and startup as well as the proposed
bridge. Keep a pre-federation native control so an earlier dispatch cost is
not silently accepted as the baseline. Establish measurement noise and retain
raw results; passing semantic conformance alone is not performance evidence.
The [roadmap native performance gate](../roadmap.md#native-execution-performance-gate)
defines the required workloads and sign-off. These are implementation and
measurement requirements, not a claim of measured parity from this document.

## 2. Public protocols

The proposed minimum protocols are:

```clojure
(defprotocol IQuerySource
  (-source-info [source])
  (-open-session [source request])
  (-plan-source [source session request]))

(defprotocol IQuerySourceSession
  (-session-info [session])
  (-open-cursor [session request])
  (-cancel-session! [session reason])
  (-close-session! [session]))

(defprotocol IQuerySourceCursor
  (-next-batch! [cursor request])
  (-cursor-metrics [cursor])
  (-close-cursor! [cursor]))
```

The source is caller-owned; a query never closes it. The provider may expose
its own `AutoCloseable` source/profile API. Sessions and cursors are opaque,
query-owned runtime objects implementing these public protocols. Their
implementations must not require core optimizer records or namespaces marked
`^:no-doc`.

Except for these runtime objects, control/reservation tokens, and explicit plan
handles, envelopes contain
ordinary maps, vectors, sets, symbols, keywords, and portable logical values.
Descriptors, session information, and alternative metadata must be immutable,
including namespaced extension fields and Clojure metadata on their values.
Mutable leaves belong in owned row values, not these metadata envelopes.
Core never evaluates provider-supplied Clojure forms or examines a plan handle.
Core must not print runtime objects, handles, raw source clauses, bindings, or
rows in automatic diagnostics.

### Version negotiation and source information

`-source-info` is metadata-only: no discovery, source-row consumption, external
request, session acquisition, or authorization side effect. Its required fields
are illustrated below. All identifiers are non-secret portable values.

```clojure
{:spi-versions #{1}
 :source-instance-id #uuid "11111111-1111-4111-8111-111111111111"
 :compute-context-id "sales/read-profile-1"
 :definition-id :sales/orders
 :definition-fingerprint "federation-definition:v1:sha256:<hex>"
 :features #{}
 :required-features #{}
 :pattern-contract
 {:kind :tuple
  :columns [{:type :long :nullable? false}
            {:type :long :nullable? false}]}}
```

The definition identifies this logical view, including its accepted metadata;
the source-instance and compute-context IDs identify its physical read domain.
Aliases, mapping versions, and connection handles are not physical identity.
Repointing at an unrelated system changes the source-instance ID. Context IDs
must distinguish incompatible catalogs, authorization profiles, and snapshot
requirements without embedding credentials.

The earlier executable v1 baseline supports two pattern contracts:

- `:tuple`: positional columns; a pattern has exactly the declared arity.
- `:eav`: exactly three terms with a constant advertised attribute, as specified
  in the [EAV descriptor contract](#eav-descriptor-contract) below.

The revised federation interface requires `:eav` under
[Section 10.1](#101-version-selection-and-compatibility). V1's positional source
contract is retained here to document existing code and tests. Ordinary Datalog
tuple inputs and the binding tuples exchanged during execution keep their own
roles under the revised contract.

Core selects the highest mutually supported SPI version and the supported
intersection of `:features` before opening a session. With no common version,
it raises `:query-source/version`. `:required-features` must be a subset of
that intersection or acquisition fails. Unknown advertised optional features
are ignored; unknown required features are rejected. Namespaced annotation
keys may be ignored and must never grant a capability. An absent optional
feature has no effect. Baseline v1 has no planning probes, borrowed batches,
aggregate delegation, ordering guarantee, or correlated execution.

Baseline sources advertise `:features #{}` and `:required-features #{}`.
Logical type names, `:tuple`/`:eav`, and baseline scan/filter/projection/join
support are not feature flags; types and pattern kinds appear in descriptors,
and exact operator support appears in planning alternatives. Optional feature
keywords and contracts are defined in the [extensions appendix](query-source-extensions.md);
additional keywords need a specified contract before use.

V1 protocol method signatures are fixed once published. Optional capabilities
add negotiated envelope fields or separate optional protocols; they do not add
required methods to an existing public protocol. An incompatible method or
semantic change requires a new major SPI contract with an explicit adapter.

`-source-info` returns a coherent immutable descriptor. Core captures it once
per source object for an invocation. Planning and execution must use that
descriptor's metadata or fail with `:query-source/stale-definition`; a provider
cannot silently replace it after refresh. Source-data changes between queries
are independent of metadata changes.

### Definition fingerprint

The provider computes `:definition-fingerprint` under the
[shared fingerprint contract](schema-definitions.md#3-revisions-fingerprints-and-data-versions),
covering the advertised `:pattern-contract`, normalized mapping/query or
equivalent definition, and its resolved dependencies. Ephemeral sources also
supply a definition fingerprint; object identity or a random token cannot replace it.

The [companion computation](../../federation/federation.md#44-fingerprint-computation)
produces the value used in its stored mapping and this envelope; there is no
separate SPI hash. Other providers own their versioned digest schemes. The
SPI requires a nonempty, non-secret string, compared as a whole for equality;
core does not hash opaque provider definitions or interpret digest internals.
Examples use `<hex>` as a placeholder for the actual digest.

Core passes the captured value unchanged to cursor opens. The provider checks
it against the definition used to plan and execute, failing with
`:query-source/stale-definition` on a mismatch. A successful refresh creates a
new accepted descriptor; it cannot update a running query's descriptor or make
an old handle valid under new metadata.

### EAV descriptor contract

An EAV source supplies the same required top-level `-source-info` fields as a
tuple source, with this `:pattern-contract`:

| Required field | Contract |
| --- | --- |
| `:kind` | Exactly `:eav`. |
| `:attributes` | A nonempty finite map keyed by every queryable attribute keyword, including any exposed identity attribute. |
| `:attributes A :entity-type` | The logical type descriptor of the entity term for attribute `A`. |
| `:attributes A :value-type` | The logical type descriptor of the value term for attribute `A`. |

Both types use [Section 3](#3-logical-values-and-clause-envelopes); they are type
descriptors, not column maps or physical type names. These are the only baseline
EAV fields in the earlier executable v1 contract; namespaced annotations may
be ignored. V1 does not advertise cardinality or reference classification beyond
the types, so core cannot infer them. The required
[v2 logical schema](#logical-schema-guarantees) supplies accepted cardinality,
entity/reference domains, and absence semantics alongside these types. Physical
expressions, uniqueness proofs, and conversion implementations stay with the
provider in both versions.

This example extends the companion's [normalized orders mapping](../../federation/federation.md#43-compilation-to-the-spi)
with a `:order/customer` reference. Its target is the same source's `:customer`
domain at epoch 0, with one `:long` key component. The `let` only constructs
ordinary data; `-source-info` returns the map with expanded type descriptors.

```clojure
(let [order-type
      {:kind :custom
       :name :datalevin.federation/entity-ref
       :version 1
       :fingerprint "entity-ref-v1-definition-hash"
       :parameters
       {:identity-domain-id [#uuid "11111111-1111-4111-8111-111111111111"
                             :order 0]
        :key-types [:long]}}
      customer-type
      {:kind :custom
       :name :datalevin.federation/entity-ref
       :version 1
       :fingerprint "entity-ref-v1-definition-hash"
       :parameters
       {:identity-domain-id [#uuid "11111111-1111-4111-8111-111111111111"
                             :customer 0]
        :key-types [:long]}}]
  {:spi-versions #{1}
   :source-instance-id #uuid "11111111-1111-4111-8111-111111111111"
   :compute-context-id "sales/read-profile-1"
   :definition-id :sales/orders
   :definition-fingerprint "federation-definition:v1:sha256:<hex>"
   :features #{}
   :required-features #{}
   :pattern-contract
   {:kind :eav
    :attributes {:order/id {:entity-type order-type :value-type :long}
                 :order/amount {:entity-type order-type :value-type :bigdec}
                 :order/customer {:entity-type order-type
                                  :value-type customer-type}}}})
```

`:order/customer` relates an order reference to a customer reference. Their
`:long` keys share a scalar type, but their nominal entity domains differ.

The custom type's name and parameter layout belong to the companion; the
fingerprint shown stands for its registered type-definition fingerprint, not a
mapping hash. Core validates the generic type envelope and compares type/domain
descriptors without interpreting those parameters. A domain keyword such as
`:order` alone is not an entity type descriptor.

Before session acquisition, core rejects malformed descriptors (including
missing type fields or invalid attribute keys) with `:query-source/invalid`.
An EAV pattern has exactly three terms; the middle term must be `[:const A]`
for an advertised attribute. Variable/wildcard attributes and unadvertised
attributes raise `:query-source/unsupported` before acquisition. Entity and
value terms may be variables, constants, or `[:ignore]`. Provider alternatives
must declare the corresponding entity/value types and `:nullable? false` for
returned pattern variables; a scalar natural key cannot replace the entity
reference type.

## 3. Logical values and clause envelopes

The initial scalar type names are `:boolean`, `:long`, `:bigint`, `:double`,
`:float`, `:bigdec`, `:string`, `:keyword`, `:symbol`, `:uuid`, `:instant`, and
`:bytes`. `:data` admits portable values without a narrower type guarantee.
Use existing Datalevin value representations and equality/hash behavior, not
provider collation, numeric coercion, array identity chosen by an adapter, or
a new SPI comparison implementation. The shared core
[`QueryKey`](../src/java/datalevin/utl/QueryKey.java) implements key equality/hash
and predicate equivalence for both execution paths. Byte arrays compare by
content, recursively through supported collections; metadata does not affect
keys. Results still contain ordinary byte arrays. Custom records and opaque
values retain their own equality. Lossy conversion is an error. Precision,
timezone, or provider types without an exact representation require an
explicit companion conversion or rejection.

`:instant` uses `java.util.Date`. Immutable `java.time.Instant`, `Duration`, and
`Period` values use `:data`, preserving nanoseconds and period components without
conversion to dates or normalized periods. They also work inside supported
collections and row metadata; descriptor/session metadata remains immutable EDN.

Each column has `:type` and `:nullable?`. A fixed tuple type is
`{:kind :tuple :elements [...]}`, where `:elements` is an ordered vector of
logical type descriptors. A registered custom type is described by
`{:kind :custom :name :app/type :version 1 :fingerprint "..."}` with optional
plain-data `:parameters` refining its logical domain. The version and
fingerprint identify the existing custom-type definition; they do not replace
the database registry or redefine equality through an order key. Custom values
must satisfy the existing portable value contract. Core compares the declared
type/domain descriptors without interpreting companion-specific parameters.

Custom-type compatibility uses the complete accepted descriptor under Clojure
map equality (`=`): `:kind`, `:name`, `:version`, `:fingerprint`, `:parameters`,
and any accepted namespaced extra fields participate. Changing the version or
type-definition fingerprint is a different type even with unchanged parameters.
Unknown unqualified fields at the descriptor's top level are invalid.
Namespaced extras and parameter entries
are preserved and compared, never stripped as annotations; put descriptive
annotations outside the logical type map. Map entry order and Clojure metadata
do not affect equality. Descriptor hashing must agree with this equality;
Java map `hashCode` can distinguish equal numeric representations.

Mapped external entity references use the companion's registered custom value
and [nominal domain/key contract](../../federation/federation.md#51-entity-identity).
They are never native `:db.type/ref` values or numeric Datalevin entity IDs.
Joining an external natural key with a native attribute remains ordinary
scalar equality. A custom value used in native storage must be registered in
that database; a source cannot silently alter the registry during planning.

For tuple sources, nullable columns may contain `nil`, which is an explicit
value. EAV facts have non-`nil` entity and value terms; an absent/SQL-null
attribute contributes no tuple, including when its pattern uses `_` or projects
away the value. There is no SPI missing-value sentinel. Row identity is explicit
data: an adapter that needs to distinguish equal physical rows exposes an
identity column. Core does not invent stable
IDs or deduplicate away `:with` variables. Existing Datalog projection and
aggregation boundaries determine observable duplicate semantics.

Virtual definitions preserve nullable tuple outputs under the analytics plan's
[output-column contract](../analytics.md#output-columns-and-nullability).
Their normalized output descriptors include both `:type` and `:nullable?`;
composition validates that contract before source acquisition and includes it
in definition/dependency fingerprints. Expansion does not turn an explicit
`nil` into an absent EAV fact or silently narrow a nullable column.

Core expands accepted virtual definitions before source planning and assigns
query-local clause IDs. The IDs are opaque to providers and stable for that
invocation. Terms use tagged vectors so constants cannot be confused with
variables or wildcards:

```clojure
{:id 0
 :kind :pattern
 :terms [[:var ?order-id] [:var ?amount]]}

{:id 1
 :kind :predicate
 :operator :>
 :semantics-version 1
 :args [[:var ?amount] [:const 50]]}
```

These are data examples, not evaluated forms. The pattern uses the declared
two-column tuple source above. `[:ignore]` represents `_`. For the EAV source,
`[$sales ?order :order/amount ?amount]` becomes:

```clojure
{:id 0
 :kind :pattern
 :terms [[:var ?order] [:const :order/amount] [:var ?amount]]}
```

Its output variables have the advertised entity-reference and `:bigdec` types,
respectively, both non-nullable even when the physical amount column is nullable.

Initially, eligible predicates are deterministic scalar `:=`, `:not=`, `:<`,
`:<=`, `:>`, and `:>=`, with their current Datalevin semantics. The semantics
version identifies the contract, not permission to use SQL's similarly named
operator. Exact support is per input type. Unsupported functions, recursion,
negation, or branch structures are not flattened into a positive conjunction.
They remain supported residual core work only when its resource and replay
requirements can be met; otherwise core rejects the query before source reads.

## 4. Query context and session lifecycle

Core establishes one query runtime containing the authorization context,
deadline, cancellation state, native read context, and shared resource ledger.
It validates syntax, definitions, source descriptors, types available from
those descriptors, and execution-mode support before acquiring source views.
Authorization and provider-specific validation occur again during session
acquisition and source planning; a reusable definition grants no access.

The common request context has:

```clojure
{:spi-version 1
 :features #{}
 :query-id #uuid "22222222-2222-4222-8222-222222222222"
 :authorization {:principal-id :application :policy-token "policy-v1"}
 :consistency :snapshot
 :control control
 :limits limits}
```

`control` is a core-owned runtime object. The public helpers in
`datalevin.query.source` are:

| Helper | Result/behavior |
| --- | --- |
| `(cancelled? control)` | Boolean cancellation state |
| `(remaining-time-ms control)` | Nonnegative time remaining, or `nil` for no deadline |
| `(check-active! control)` | `nil`, or throws cancellation/existing query-timeout error |
| `(register-cancel! control cancel-fn)` | Register a thread-safe cancellation signal function and return an opaque registration |
| `(unregister-cancel! registration)` | Idempotently remove a registration; return `nil` |
| `(reserve! control charges)` | Atomically charge a map of resource names to nonnegative amounts; return an opaque reservation token, or throw without charging any category |
| `(release! reservation)` | Idempotently release that token's live-resource charges; return `nil` |
| `(batch-charge-bytes columns rows)` | Conservative nonnegative retained-value/container charge, or an unsupported-value error |

Control, cancellation registrations, and reservation tokens are query-scoped
runtime objects, not portable logical values; they are never persisted or
rendered in diagnostics. Time
remaining uses the query's effective deadline, including inherited nested
timeouts. Reserve before allocation/work. Resource names are the limit keys in
Section 7, excluding per-batch caps checked on each batch. `:external-requests`
is cumulative and is not refunded by release; the other ledger categories
measure live use. A provider returning a batch transfers its reservation to
core through an opaque `:reservation` batch field; core releases it when that
batch is consumed or its retained state is independently accounted. Failed
operations release their own reservations before throwing.

Register cancellation before a blocking acquisition or operation, including
connection checkout and snapshot acquisition before a session exists. The
callback receives the redacted `{:error ... :phase ...}` cancellation reason
and signals backend cancellation without waiting for the blocked operation's
execution lock. Core dispatches callbacks from another execution context.
Concurrent registration and cancellation cannot lose a signal; registering
after cancellation dispatches immediately. Unregistration prevents future
dispatch but may race an already dispatched callback, so callbacks are
idempotent and safe during cleanup. Unregister on completion or acquisition
failure. Callbacks return promptly; failures do not replace the primary query
failure. Core clears remaining registrations during query cleanup. A provider
must reject acquisition that it cannot make deadline/cancellation-aware.

`-open-session` receives this context plus `:source-descriptors`, the complete
set of captured logical views that will share the session. It must authorize
their access, acquire a stable read view, and return an `IQuerySourceSession`.
If acquisition fails, the provider closes everything it acquired before
throwing. Core cannot close a session it never received.

Acquiring a one-shot input reserves its read view and ownership without
traversing it. If this cannot be guaranteed, reject snapshot mode. A selected
query-local spool may consume that input only after planning succeeds.
Snapshot copies of replayable mutable inputs are acquisition work and must
obey the same memory/spill limits.

`-session-info` is metadata-only and reports:

```clojure
{:spi-version 1
 :features #{}
 :session-id #uuid "33333333-3333-4333-8333-333333333333"
 :source-instance-id #uuid "11111111-1111-4111-8111-111111111111"
 :compute-context-id "sales/read-profile-1"
 :authorization-token "effective-policy-v1"
 :consistency :snapshot
 :acquired-at-ms 0
 :data-version nil}
```

The effective authorization token is a non-secret identity/fingerprint, not a
credential. Core shares a session only for matching source-instance,
compute-context, effective principal/policy, and consistency requirements, with
compatible declared logical views. Repeated source aliases do not acquire
independent snapshots. If the provider cannot serve the group in one read view,
the query fails instead of opening unrelated snapshots. Core may serialize
cursor work within that session; concurrent cursor support is not required.

`:snapshot` is the default. It covers every fragment, repeated scan, and bound
request for that source. Mutable local input needs a provider-guaranteed
snapshot or quota-bound copy/spool; retaining its object reference is
insufficient. The native Datalevin input must also have a query-consistent
read context. This does not promise a global snapshot across independent
systems. A lost view fails the query; retries cannot switch views midway.

Explicit `:statement` or `:best-effort` requests may be accepted and must be
reported as such. No silent downgrade is permitted. Weak views do not permit
correctness shortcuts based on previous reads or definitions requiring stable
replay. `:data-version`, if present, identifies the exact pinned data view within
its source/compute context under the provider's version contract. Use `nil` when
the provider cannot supply such a token, including weak modes without a pinned
view. Core assumes no ordering or cross-provider comparability. It is not a
schema/definition fingerprint, session ID, maintenance log position, or promise
of result-cache reuse.

Non-nil data-version tokens are non-secret portable identifiers suitable for
diagnostics, like definition IDs. Their semantic values may appear in explain;
private annotations belong outside those values. Clojure metadata on an
identifier is not part of its diagnostic representation.

Core completes acquisition and session metadata validation for every source
before planning any of them. The normal lifecycle is:

```text
capture descriptors -> validate -> acquire/authorize sessions
  -> plan each source -> validate/select plan -> open selected cursors
  -> consume owned batches incrementally -> close cursors -> close sessions
```

On failure, core signals cancellation, then closes every acquired cursor and
session in reverse ownership order. Closing a session also releases any
unclosed child cursors as a defensive fallback. Cleanup is idempotent; cleanup
failures are suppressed onto the primary failure. Session close releases its
read transaction and query state, never a caller-owned pool or source profile.
Cancellation may race with a blocking operation and must be thread-safe.
Other calls on a session/cursor are serialized by core; thread affinity is not
promised, and providers marshal calls internally when their backend needs it.
Core must observe deadlines/cancellation while a provider call is blocked and
signal registered callbacks and acquired sessions from another execution
context. Close/cancel methods return `nil`; repeated calls are harmless. Work
attempted after close raises `:query-source/invalid` with `:state :closed`.
Metadata and final cursor metrics
remain readable after close.

## 5. Planning request and alternatives

Core calls `-plan-source` once per explicit source variable with all of its
eligible clauses, using that source's captured descriptor and shared session.
Different variables may share a session while retaining separate planning
calls and clause identities. No per-pattern planning call or hidden planning
scan is permitted. Planning consumes accepted metadata and request constants;
baseline v1 cannot issue counts, samples, source `EXPLAIN`, or discovery I/O.

The request extends the common context:

```clojure
{:source-symbol $sales
 :source-descriptor source-info
 :clauses [pattern-clause predicate-clause]
 :bindings {?minimum 50}
 :required-vars #{?order-id ?amount}
 :demand {:quality :exact :ordering [] :offset 0 :limit nil}
 :max-alternatives 32}
```

`:bindings` contains known scalar values only, not source handles or an
unbounded outer relation. `:required-vars` includes join keys, `:with` inputs,
and residual dependencies, not just final output columns. Demand describes
the final query; it does not authorize a source-local limit or offset.

The response is `{:alternatives [...]}`. An alternative has:

```clojure
{:id :scan-1
 :plan opaque-plan-handle
 :covers #{0}
 :prefilters #{1}
 :requires []
 :columns [{:variable ?order-id :type :long :nullable? false}
           {:variable ?amount :type :long :nullable? false}]
 :properties {:replay :reopen
              :ordering []
              :blocking-state :none}
 :estimate {:rows 1000 :row-bytes 32 :startup-ms 1 :per-row-ns 100
            :requests 0 :confidence :low}
 :explain {:label "declared relation scan"}}
```

`:covers` contains clauses implemented exactly. `:prefilters` contains
additional predicates applied only as proven superset filters; these remain
core residual checks. The two sets are disjoint. Partial coverage of a pattern
does not count as exact coverage. A provider cannot remove a predicate merely
because an estimate suggests all rows satisfy it.

Core validates known clause IDs, unique alternative IDs and column variables,
types, supplied/required bindings, residual columns, finite nonnegative cost
values, capability claims, and disjoint coverage in the chosen plan. Unknown
cardinality, width, or cost components are `nil` with `:confidence :unknown`,
never optimistic zero. Estimates are advisory, including an estimated zero.
Provider claims are trusted code claims tested by conformance; structural
validation cannot prove a SQL translation sound. The revised baseline's
conditioning, comparable units, and whole-plan ranking are specified as work
in [Section 1.2](#12-capability-and-cost-planning-target); this existing envelope
does not establish that implementation.

A variable repeated in covered patterns has one output column with the common
logical type. Its `:nullable?` is true only when every covered occurrence permits
`nil`; column order cannot weaken that constraint.

Every required external pattern must be exactly covered once by selected
alternatives. Core may combine alternatives with disjoint coverage and local
joins when that plan is bounded. It never sends an uncovered source pattern
through collection lookup, drops it, or treats no alternatives as no rows. No
complete admissible plan raises `:query-source/unsupported` before opening an
execution cursor or consuming a one-shot input.

Rechecks run before distinct, limit, aggregation, or other lossy operations.
V1 does not delegate those operations or promise source ordering. Providers
preserve every required row/identity and residual column. In the earlier executable
baseline, `:requires` is empty; core chooses a bounded scan/local join or rejects the plan, never one
remote cursor per outer row. SPI v2 defines required inputs and cursor-open
binding batches in [Section 10.5](#105-dependent-access-and-binding-batches),
superseding the earlier [bulk-binding proposal](query-source-extensions.md#1-bulk-bindings).
This documentation change does not implement it; §§2–9 remain the earlier
baseline.

All v1 source alternatives and opaque handles are query-scoped. They must not
be persisted or retained in shared native plan caches, and cannot own open
cursors or resources requiring individual disposal. Query resources belong to
the session. Reusable provider plan descriptions and planning probes require
separately negotiated extensions; neither is necessary for v1 execution.
Core admits authorization, descriptors, session information, and every planning
envelope against the shared retained-memory quota before recursive validation.
Sizing and validation observe cancellation inside nested metadata. Charges include
extension fields, Clojure metadata, and hidden vector backing; explicit `:plan`
handles remain opaque. Core releases session-information charges after validation
and response-header/unselected-alternative charges after selection. Selected
envelopes remain charged through execution or explain. Providers account for the
state behind handles through their session.

## 6. Cursor requests and owned batches

`-open-cursor` receives exactly the seven [common context](#4-query-context-and-session-lifecycle)
fields (`:spi-version`, `:features`, `:query-id`, `:authorization`, `:consistency`,
`:control`, `:limits`) plus these four fields in baseline v1:

```clojure
{:alternative-id :scan-1
 :plan opaque-plan-handle
 :source-symbol $sales
 :definition-fingerprint "<captured fingerprint>"}
```

Core forwards the captured context unchanged and the selected handle by identity.
The fingerprint is the selected source's captured definition fingerprint.
Acquisition/planning fields (`:source-descriptors`, `:source-descriptor`,
`:clauses`, `:bindings`, `:required-vars`, `:demand`, `:max-alternatives`) are
absent. During planning the provider associates required clauses, bindings,
and other execution state with the session-owned handle; opening it does not
replan or supply new bindings. Additional execution inputs require a specified,
negotiated extension.

Open uses the session's existing view and returns an `IQuerySourceCursor`.
Failed open cleans up partially acquired cursor resources. Opening unselected
candidates for speculative execution is not part of v1.

`-next-batch!` receives `{:control control :max-rows n :max-bytes b}`. These are
the remaining permitted batch sizes, not hints. Calls pull one batch at a time;
there is no background unbounded queue. The response is:

```clojure
{:columns [{:variable ?order-id :type :long :nullable? false}
           {:variable ?amount :type :long :nullable? false}]
 :rows [[101 75] [102 90]]
 :row-count 2
 :byte-count batch-byte-charge
 :reservation batch-reservation
 :exhausted? false}
```

The columns exactly match the selected alternative, in order. Rows are owned,
fixed-arity vectors of logical values. Counts must match the rows and obey the
requested limits. `batch-byte-charge` is the charge computed for these rows;
`batch-reservation` is a live token covering at least that many retained bytes
and one in-flight batch on this query's ledger. Core validates the token and
accepts its ownership once. A provider never releases a transferred token from
cursor/session close. For an empty final batch the token may be omitted when
its computed charge is zero; row metadata or backing storage still needs a
reservation.
The final batch may contain rows with `:exhausted? true`.
An empty non-final batch is invalid; the provider continues its bounded read
internally or throws on cancellation/limit. `nil` is not an exhaustion marker.
After exhaustion, core closes the cursor and makes no further batch call.

V1 transfers ownership of batch values to core. They remain valid after the
next advance and cursor/session close. Providers copy reusable buffers and
mutable byte arrays before returning them and do not mutate returned values.
Arrow memory, JDBC buffers, lazy sequences, and foreign pointers never cross
as logical values. Borrowed batches are deferred to a negotiated extension;
there is no release-batch protocol in v1.

`:byte-count` is a conservative charge for retained logical values and batch
containers, not wire bytes or a claim about total JVM heap usage. Core supplies
a public `batch-charge-bytes` helper and validates the charge; unknown/custom
value sizes require an explicit conservative charge or rejection. Core checks
row and advertised byte limits before traversing values, then recomputes the
charge incrementally under the byte cap with cancellation checks, including
nested values and metadata. An understated count cannot bypass the cap; the
reported `:attempted` charge may be the first subtotal exceeding it. The accounting
must include retained backing storage, not just a slice's visible length.
The shared helper charges a Clojure `subvec`'s entire parent, including hidden
elements and parent metadata, wherever the view occurs in rows or columns.
Shared backing is conservatively counted per occurrence. `owned-copy` detaches
vector views; its `copy-charge` companion sizes that detached result before
allocation. Providers report the helper's charge for the actual handed-off
values, even if they reserved more before copying. Core input and constant
capture use the detached charge; retained join and result values keep their
backing charge.
Oversized individual values fail with `:query-source/limit` rather than bypass
the batch cap. Final defaults and type-size accounting must be fixed in the F1
implementation and tested with large strings, arrays, and custom values.

`:replay :reopen` permits a new cursor for the same plan in the same session
with identical logical rows; ordering is still unspecified. For the baseline,
selecting `:replay :once` permits only one source cursor for that session across
all alternatives and aliases. Core rejects a plan requiring additional reads,
or explicitly spools once under quotas and reports that spool. A provider that
supports more independent reads can expose a replayable session snapshot.
SPI v2 replaces that session-wide restriction with the explicit
[consumption groups](#104-consumption-groups-and-replay) in Section 10.4.
No implicit sampling or replay is allowed in either version.

`-cursor-metrics` is non-blocking and returns cumulative `:batches`, `:rows`,
`:charged-bytes`, `:read-rows`, `:read-bytes`, `:external-requests`, and
`:elapsed-ns`, including final counts after close. Unknown physical read work
is `nil`, not zero. Session setup is charged to the query ledger separately
and is not attributed to each cursor. Metrics are observations, not a second
result channel.

## 7. Resource limits, early stop, and cancellation

For queries using this SPI, the proposed query-map option is `:query-source`:

```clojure
{:query-source
 {:consistency :snapshot
  :limits {:batch-rows 4096
           :batch-bytes 1048576
           :in-flight-batches 2
           :retained-bytes 67108864
           :spill-bytes 1073741824
           :result-rows 1000000
           :result-bytes 67108864
           :external-requests 1024
           :concurrent-requests 4}}}
```

These numbers are proposed initial defaults, subject to F1 measurements; the
field meanings and enforcement are the contract. All limits are finite
nonnegative integers, except positive batch/concurrency sizes. Zero spill means
spilling is disabled. `:timeout` retains its existing query meaning and is not
a source-local timer. Native-only query behavior is unchanged.

Limits apply to the complete expanded query, including native join state and
residual operators on the external path, not separately per provider,
alternative, cursor, or nested query. A nested query shares the ledger and may
tighten limits but cannot reset spent work or extend its parent's deadline.
Source-owned in-flight work, batches retained by core, blocking state, spools,
and final-result materialization are accounted separately without silently
omitting one category. Allocations transfer reservations as ownership moves;
the same live batch is not charged twice to one category.

An external request is a provider/backend operation such as executing a
statement or fetching another result page, including session setup I/O.
Providers reserve request/concurrency allowance before issuing it and report
actual work. Independent lower backend limits may split requests, consuming
the shared allowance. Physical rows examined by an external engine may be
unknown and are not confused with rows transferred or retained memory.

Core implements incremental consumers for non-blocking operations. Blocking
hash builds, distinct, grouping, and sorting reserve state and spill under the
disk quota where supported, or fail explicitly. A cursor does not make existing
full-result collectors bounded. Unsupported operator/replay shapes are rejected
before consuming a one-shot cursor. Bad estimates cannot disable runtime limits.

`d/q` completes query-source execution and materializes its result before
returning. In the initial F1 executor, a relation find without a return map
returns a vector of row vectors when it contains an aggregate or a nonempty
`:with`, including `[]` when no result rows remain. These vectors preserve
native result values and multiplicities under `=`, but differ from native
`d/q`'s lazy sequences for those queries; concrete collection-type parity is
not guaranteed. Plain relation finds return a spillable set, and relation
finds with `:keys`, `:strs`, or `:syms` return a set of maps, including when
combined with aggregates or `:with`.

A result limit is an overflow error, never silent truncation. A query `:limit`
is a semantic operator applied after required joins and residual checks;
reaching a safe final limit stops source reads and closes cursors promptly.
A result-size quota alone cannot authorize early success.

Providers check `control` before opening/fetching and during long decoding or
computation. `-cancel-session!` signals all outstanding work and must unblock
pending I/O using the backend's cancellation/close mechanism. It is safe to
repeat, including after close. It must not wait for the cancelled operation's
own execution lock. Its `reason` is a redacted map containing `:error` and
`:phase`, never the raw primary exception. Cancellation, quota overflow,
snapshot loss, timeout, and provider errors fail the whole query; already
received rows are never returned as an unmarked partial success.
Core keeps a session's cancellation registration active until its close call
finishes, then unregisters it even if close throws. Closing may wait for pending
provider work, so cancellation must also unblock that wait. Completed core
cleanup entries drop their resource and runtime references; repeated close
calls remain idempotent and cleanup errors preserve the primary failure.

## 8. Caching, errors, and explanation

Any expanded dependency on an `IQuerySource` bypasses query-result caching
before lookup and insertion, including rules, virtual definitions, and nested
queries. Source object identity, mapping revisions, metadata fingerprints, and
session IDs are not data-freshness tokens. Source authorization is performed
on every invocation. Initially, shared caching of external plans, rows, and
planning evidence is also disabled; native-only caches retain their current
behavior.

Errors use `ex-info` with a stable `:error` category and redacted structural
context: `:query-source/version`, `:query-source/unsupported`,
`:query-source/invalid`, `:query-source/unauthorized`,
`:query-source/stale-definition`, `:query-source/snapshot-lost`,
`:query-source/limit`, `:query-source/cancelled`, and `:query-source/provider`.
Timeout preserves the existing `:query/timeout` classification. Add `:phase`,
`:query-id`, `:source-symbol`, and applicable `:clause-ids` and `:alternative-id`.
The embedded executor uses zero-based `:where` clause indices. Context comes
from the query and accepted plan, never from provider exception data; an
alternative ID is included once an alternative is selected. Shared-session
acquisition and cleanup identify the alias used to acquire the session.
Limit errors also name the category, maximum, and attempted charge. Native provider
exceptions and SQL can contain secrets; automatic diagnostics must not render
their messages/causes without provider redaction. Cleanup errors do not replace
the original category.

`explain` without execution may acquire/authorize a session and plan, and must
close it before returning. It opens no execution cursor; session-acquisition
work is identified separately. The source executor honors `:run?` (default
`false`) and `:intermediate-counts?` (default `true`). With execution enabled,
the latter controls per-row counting and the `:streamed-rows` field; disabling
it retains final result sizes and provider cursor metrics. Plan-only explain
and ordinary `q` calls do not collect intermediate counts. Nil explain options
mean the default plan-only request. Unknown source explain options reject with
`:query-source/unsupported` in phase `:explain` before contacting a provider.
Execution explanation reports selected clause
coverage, residual checks, metadata versions, actual consistency, blocking
operators/spools, estimates, actual rows/bytes/batches/requests, and limit
failures. An allowlisted provider `:label` may be shown; opaque plans, SQL,
credentials, raw bindings, rows, and source values are never printed. No-query-
source queries allocate no extension sessions, ledgers, or planning state.

Each `:sources` entry reports the validated session's `:consistency`. Its
`:selected` entries report exact `:covers` and `:prefilters` as clause-ID sets
and include a public `:alternative-id` and the validated `:estimate` envelope
from §5, preserving unknown values
and confidence separately from actual `:cursors` metrics. Selected-plan display
containers are quota-accounted and omit provider extension fields and Clojure
metadata, including when metadata printing is enabled.

The earlier embedded baseline reports the following execution details:

- `:acquisition {:sessions n}` counts acquired sessions separately from execution
  cursors, including in plan-only explain. Shared aliases count as one session.
  Session setup work is not inferred from or added to cursor metrics.
- `:operators` describes planned core placement: relational `:inputs` by input
  index, ordered `:readers` with source/clause/alternative IDs and `:retained?`,
  shared-variable join indexes, distinct on find/with variables, optional
  grouping, and retained results. Its `:storage :memory`, `:spill-policy :reject`,
  and `:replay-spools :none` describe the current executor policy. Selected
  provider blocking/replay properties remain under `:sources`.
- Executed output adds `:actual-operators`: input row counts, retained-reader
  row counts in planned order (a prefix when an empty table stops execution),
  indexes and cross products actually built, distinct rows admitted, and
  actual `:spools` and `:spill-bytes` (both zero in this implementation).
  An empty stream builds no join indexes. Plan-only output omits these counters.
- Each `:cursors` entry identifies its `:source`, `:alternative-id`, and
  `:clause-ids`, and contains the seven allowlisted metrics from §6. Unknown
  metrics remain `nil`; unknown fields and Clojure metadata are omitted.
  Rows/bytes count batches delivered by that cursor, including unused rows in
  the last batch after an early limit. They are not final result sizes.
- `:result-rows` counts retained output tuples after aggregation and the final
  window; `:result-bytes` is their conservative result-ledger charge, including
  container overhead, not serialized bytes. Both remain available with
  intermediate counts disabled. Plan-only output omits result sizes and cursor
  metrics; the existing `:native-scans 0` records that no native scans occurred.

Core display containers and copied identifiers are admitted against the query
budget. A failed execution throws the redacted error, including applicable
limit details; it does not return a partial successful explanation. Primary
and suppressed cleanup errors strip provider messages, causes, annotations,
and extension fields while preserving public structural context.

The same entry identifies the captured logical view with `:definition-id` and
`:definition-fingerprint`, and separately reports the acquired session's
`:data-version` (`nil` when unavailable). These values come from the accepted
descriptor and session metadata, never a later provider refresh. Displayed
identifiers are copied under the query budget, detached from collection backing,
and stripped of Clojure metadata recursively. Schema revisions and definition
update counters are not baseline SPI fields and are not inferred from these
values or copied from provider annotations; their roles follow the
[shared version vocabulary](schema-definitions.md#3-revisions-fingerprints-and-data-versions).

The top-level `:residual-checks` vector describes the predicates core evaluates
on joined rows, in query clause order:

```clojure
{:clause-id 3 :operator :> :source-symbols [$orders $limits]
 :stage :before-projection}
```

`:source-symbols` lists the symbols of sources whose patterns bind the predicate's
variables, including native sources, sorted by symbol. It is empty when no source
pattern binds the predicate's variables, for example predicates over only query
inputs or constants. Core currently rechecks every predicate, including exact
provider coverage and superset prefilters. `:before-projection` places
these checks before core projection, aggregation, and final limit/offset.
The same structural description appears in plan-only and executed explain;
it contains no operands, bindings, or predicate values.

## 9. Source conformance

Acceptance follows the [milestone checklist](../roadmap.md#source-milestones);
its [F1/core row](../roadmap.md#f1-core) defines the shared execution suite.

The [public namespace](../src/datalevin/query/source.clj) implements the three
protocols, descriptor validation, and control/accounting helpers. The
[test-only fake and driver](../test-src/datalevin/query/source/) exercise these
without loading the query engine or a provider. Run the isolated F0 suite with:

```sh
clojure -M:test -m datalevin.query.source.runner
```

The F0 driver eagerly retains small fixtures. The separate
[F1 executor](../src/datalevin/query/source/execute.clj) now accepts explicit
source inputs in `d/q` and `d/explain`, bypassing native result caches. Run its
focused integration suite with:

```sh
clojure -M:test -m datalevin.query.source.execution-runner
```

Run [native isolation checks](../test-src/datalevin/query/native_isolation_runner.clj)
in separate JVMs; they deliberately install protocol extensions and check both
public-SPI/core load orders:

```sh
clojure -M:test -m datalevin.query.native-isolation-runner
clojure -M:test -e '(require (quote datalevin.query.source)) (require (quote datalevin.query.native-isolation-runner)) (datalevin.query.native-isolation-runner/-main)'
```

These checks cover direct/extended sources, native DB precedence, native syntax,
caches and prepared reads, lazy executor loading, and rejection of native-image
source execution before lazy resolution. The last check uses the JVM execution
mode property; it does not certify a native-image build. The separate
[measurement script](../script/query_source_native_gate.clj) compares native
entry points with a reconstructed control that removes source routing. It
records raw timings, thread allocations, loading, GC, and the control source;
it supplements the [performance gate](../roadmap.md#native-execution-performance-gate)
and does not replace its historical control or complete workload requirements.

The [shared execution matrix](../test-src/datalevin/query/source/conformance.clj)
is test support, separate from the runtime SPI. It accepts an adapter constructor
and a row-replacement function. Core runs it against the fake; the companion
runs the same checks against its local adapter with `clojure -M:test:local`.
Deterministic seeds and batch sizes exercise all thirteen baseline scalar types,
tuple/EAV queries, aliases, native joins, input bindings, grouping, `:with`,
nulls, bytes, and numeric keys.
Comparisons preserve key distinctions and multiplicities instead of using
Clojure set equality. Explicit expected results also check the eager oracle.
Repeated tuple variables exercise every nullability combination across two or
three columns. Test nominal types cover scalar/composite references, reference
attributes, missing facts, domain/epoch mismatches, and natural-key joins to
native data through both fused and separate source fragments.
Reference variants also pin version, fingerprint, name, and accepted extra-field
identity, equivalent reordered/metadata-decorated descriptors, hash-based
distinctness, and rejection before acquisition. These JVM fixtures do not establish
the companion's F2 reference ordering or wire encoding.
Public protocol wrappers check pinned views under replacement, acquisition of
all views before planning, and snapshot/cancellation isolation between concurrent
invocations. Cursor-open checks pin the exact baseline request keys, captured
context, and selected handle identity through query and explain, including
aliases and scalar bindings; the isolated F0 harness checks the same envelope.
They also cover plan-only explain, acquisition/planning/open/fetch
faults, limits, early stop, and resource release. Blocking wrappers exercise
cancellation before and after session/cursor/batch handoff, after partial fetch
progress, and during cursor/session close. Explicit cancellation and real nested
deadlines run through query, executed explain, and applicable plan-only paths,
with and without aliases. They check callback dispatch off the query thread,
error location, close order, released reservations, and a successful fresh query
using the same source. Adapter-specific fault and capability tests remain necessary.

The matrix's `explanation!` checks plan-only and executed output for estimates
and actual consistency. It covers fused/separate plans and shared aliases,
unknown/partial/numeric estimates, zero estimates with real rows and quota
failures, invalid estimates, mismatched or unsupported consistency, snapshot
loss, fresh reuse, and redaction of private annotations under metadata printing.
Its `coverage-explanation!` checks selected coverage and core residuals across
fused/separate plans, cross-source and native joins, input-only predicates, and
deliberately permissive prefilters. Eager comparisons and explicit results pin
filtering before aggregation and limit/offset; plan-only output opens no cursor,
and predicate constants, bindings, and private metadata stay out of diagnostics.
The `metadata-explanation!` checks publish row-only, definition, and counter-only
refreshes during acquisition, planning, and fetching. Real fake/local sessions
keep their captured versions across aliases and fragments; later invocations
see the new versions. They also cover unavailable data tokens, nested private
metadata, retained version charges after releasing the session envelope, and
oversized-version rejection before planning.

`execution-explanation!` reconciles reported rows, batches, and charged bytes
with real adapter events across aliases/fragments, empty inputs/results, joins,
grouping, and early limits. It checks planned versus allocated operators and
final sizes with intermediate counting disabled. `diagnostic-boundary!` checks
batch, retained-state, result, and request limits; unknown/invalid metrics;
and primary/suppressed failures. It injects private metadata, labels, opaque
handles, row values, and exception causes and checks metadata-aware redaction
through both adapters. These checks run as part of the same lifecycle suite.

The [integrated failure regressions](../../dtlvtest/test/datalevin/query/source_failure_test.clj)
run with `lein test datalevin.query.source-failure-test` from `../dtlvtest`.
They exercise invalid descriptors/plans/batches, one-shot selection, partial
opens, lost views, late cancellation, and cleanup error preservation through
`d/q` and `d/explain`, checking that query resources are released.
They also reject mutable descriptor/session/plan metadata before the next
provider operation and check metadata quotas across acquisition, planning, and
aliases, including plan-only explain. Oversized authorization, session envelopes,
unselected alternatives, and hidden backing reject before later values are walked;
transient charges are released before execution. Explicit plan handles stay opaque.
Batch validation
regressions cover early envelope rejection, understated byte counts, cancellation
inside nested values and metadata, and cleanup after partial progress.
Core row-sizing regressions also interrupt scratch, distinct/grouping, and output
charges inside nested values, and reject insufficient scratch space before an
unneeded traversal. The [runtime tests](../test/datalevin/query/source_runtime_test.clj)
check exact scaled charges, result quotas, and arithmetic overflow.
Validation cases cancel inside captured input values and constant metadata,
including native type checks and planning requests/responses. Cancellation keeps
its category and originating source/clause. Executable fake alternatives exercise
backtracking, overlapping/incomplete coverage, and one-shot restrictions through
both query and explain. Cases include equivalent failed paths before a valid
fallback, predicate-sensitive coverage, residual checks before aggregation/windows,
shared aliases, and later-fragment failure/cancellation. Search workspace is
released before cursor execution; every case checks query cleanup.
Close-phase cases keep cancellation active through session shutdown, including
shared aliases, plan-only explain, and nested deadlines. The
[core cleanup tests](../test/datalevin/query/source_runtime_test.clj) check
reference release, concurrent/reentrant close, cleanup order, and redaction.

The [explain-option regressions](../../dtlvtest/test/datalevin/query/source_explain_test.clj)
run with `lein test datalevin.query.source-explain-test` from `../dtlvtest`.
They cover counting defaults and opt-out, plan-only execution, invalid options,
and native/source option parity.

The [multi-row batch regressions](../../dtlvtest/test/datalevin/query/source_batch_test.clj)
run with `lein test datalevin.query.source-batch-test` from `../dtlvtest`.
They use the F0 fake's multi-row cursor to check row/byte boundaries, reservation
transfer and release, early stop, and rejection of oversized batches,
independently of the companion local adapter.

The companion's [batch and memory regressions](../../federation/local/test/datalevin/federation/local_batch_test.clj)
check snapshot charges including outer-vector metadata, early rejection during
nested sizing, and cancellation while sizing snapshots/plans/cursor scratch
and copying batch values. Sizing uses the request's byte cap; the final atomic
reservation enforces the shared ledger. A partial batch leaves its next row
unconsumed, and a failed copy releases its reservation. The adapter retains one
reservation per output batch.

The [input-binding regressions](../../dtlvtest/test/datalevin/query/source_input_test.clj)
compare tuple, collection, and relation inputs with eager queries, including
duplicates/`:with`, nulls, native joins, mutable iterators, quotas, cancellation,
and cleanup before provider acquisition.

The [constant-ownership regressions](../../dtlvtest/test/datalevin/query/source_constant_test.clj)
mutate caller-owned dates and nested value metadata during input iteration and
provider callbacks. They check fused/residual predicates, native joins, aliases,
fresh invocations, quota rejection before provider calls, redacted plan-only
explain, and cleanup.

The [capture regressions](../../dtlvtest/test/datalevin/query/source_capture_test.clj)
exercise incremental sizing and cancellation inside constants, flat inputs,
and nested metadata. They check early rejection before oversized container
children, cancellation during copying, shared-ledger changes, size overflow,
and release of partial progress before provider callbacks, including plan-only
explain.

The failure and input-binding suites also cover retained vector views: hidden
parents cannot bypass batch/result limits, backing traversal observes
cancellation, and copied constants/inputs neither inspect nor retain hidden
parent values. Empty row vectors with metadata or backing storage need a live
reservation even when they contribute no rows.

The [join-index regressions](../../dtlvtest/test/datalevin/query/source_join_test.clj)
run with `lein test datalevin.query.source-join-test` from `../dtlvtest`.
They cover shared and composite keys, collisions, duplicates, nulls, cross
products, native joins, and cleanup; equality-call counts detect quadratic
probes without timing assertions. The focused core suite also checks index
reservation failures and cancellation during construction.
The [native key-equality regressions](../../dtlvtest/test/datalevin/query_key_equality_test.clj)
check decimal scale and tuple keys across bounded entity/value/full scans,
domain filtering, reference aliases, and eager input relations.

The [aggregate regressions](../../dtlvtest/test/datalevin/query/source_aggregate_test.clj)
compare native reductions and check cancellation, arithmetic workspace, and
result quotas. They cover decimal scale/precision expansion, wide integers,
ratios, independent groups, native/source joins, and cleanup on every exit.

The [EAV execution regressions](../../dtlvtest/test/datalevin/query/source_eav_test.clj)
compare fused plans and separate core-joined fragments with eager facts through
`d/q`. They cover nominal entity domains and epochs, reference-valued attributes,
scalar/composite natural-key joins to native data, typed inputs, missing values,
duplicates/`:with`, and pinned aliases. Small graphs exhaust reference/name
presence combinations. Invalid domains, attributes, batches, and nullable EAV
columns check rejection and cleanup; native type errors retain their source
symbol and clause IDs.

The [empty-join and window regressions](../../dtlvtest/test/datalevin/query/source_short_circuit_test.clj)
verify that an empty required join table stops subsequent native scans and
source cursors, including giant-value decoding and shared-session fragments.
They check empty result shapes, cleanup, planning/cancellation failures, result
overflow, and dynamic limit/offset validation against eager native evaluation.

The [aggregate cancellation regressions](../../dtlvtest/test/datalevin/query/source_aggregate_test.clj)
check cancellation, interruption, and deadlines after cursor exhaustion,
during grouping and aggregate reduction, including multiple aggregate passes.
They also check cleanup, result quotas, and eager parity for numeric aggregates,
null grouping keys, `:with`, and result windows.

The [decimal projection regressions](../../dtlvtest/test/datalevin/decimal_projection_test.clj)
compare native, eager, and source results for scalar and tuple keys, aggregates,
`:with`, and windows. They also check memory/disk result-set iteration, key
collisions, mutations, and the [spill codec's compatible payloads](nippy.md#values-and-existing-extensions).

The [native tuple regressions](../../dtlvtest/test/datalevin/query/source_tuple_test.clj)
run with `lein test datalevin.query.source-tuple-test` from `../dtlvtest`.
They compare mixed joins and aggregates with eager results for inline/giant
tuples, inferred composite keys, native reference components, and missing
components, and check decoding reservations on success and failure.

The [native existence regressions](../../dtlvtest/test/datalevin/query/source_existence_test.clj)
check explicit value wildcards against byte, custom, and generic attributes,
including cardinality-many facts, missing attributes, bound entities, and
`:with`. They verify small-budget scans over giant values, skipped custom
callbacks, pinned views across concurrent writes, early stop, and cleanup.

The [native entity regressions](../../dtlvtest/test/datalevin/query/source_entity_test.clj)
check literal and scalar/tuple/collection/relation-bound IDs under small memory
budgets, unindexed attributes, cardinality-many facts, missing entities/attributes,
pinned aliases, explain, and cleanup. They also cover source-provided IDs,
reference chains, null candidates, duplicate/correlated rows, and early stop
between entity ranges. Native list and streaming readers
also return empty for unknown attributes and refresh after schema creation.

The [attribute-scan regressions](../../dtlvtest/test/datalevin/query/source_attribute_test.clj)
count real native cursor entries to verify selective AVE scans and bounded EAV
duplicates. They cover unindexed attributes, missing attributes, giant prefix
collisions, attribute-ID boundaries, early stop, cancellation, and cleanup.
Value-seek cases also check scalar constant/input equality, signed zero, NaN,
decimal/tuple fallbacks, and reservation before bound encoding.

The [native custom payload regressions](../../dtlvtest/test/datalevin/query/source_custom_test.clj)
cover registered types using portable Nippy data: colliding order keys, typed
inputs, unindexed/many-valued attributes, compressed and cached payloads, quota
failures, pinned aliases, ownership, and cleanup. They also check reads after
reopening without the order runtime and rejection without calling custom codecs.

The [variable tuple regressions](../../dtlvtest/test/datalevin/query/source_variable_tuple_test.clj)
run with `lein test datalevin.query.source-variable-tuple-test` from `../dtlvtest`.
They cover varying arities, inline unsigned counts/lengths, compressed giants,
native references, owned mutable components, and reservation cleanup.

The [native data regressions](../../dtlvtest/test/datalevin/query/source_data_test.clj)
run with `lein test datalevin.query.source-data-test` from `../dtlvtest`.
They cover structured values and untyped composite keys, compressed giant
values, cached expansion, and cleanup after quota or unsupported-value errors.
The [portable data regressions](../../dtlvtest/test/datalevin/query/source_portable_data_test.clj)
add characters, ratios, smaller JVM integers, and default sorted collections,
including eager join/aggregate parity and malformed-value cleanup.
The [Java time regressions](../../dtlvtest/test/datalevin/query/source_time_test.clj)
cover nanosecond precision, extreme values, constant/flat inputs, predicates,
aggregates, compressed nested data, quotas, and cleanup after malformed values.

The [legacy compression regressions](../../dtlvtest/test/datalevin/query/source_compression_test.clj)
run with `lein test datalevin.query.source-compression-test` from `../dtlvtest`.
They cover LZ4, Snappy, Zstd, and LZMA2 inline and giant payloads, nested storage
compression, expansion/dictionary limits, cached collections, and cleanup after
partial scans.

The [typed metadata regressions](../../dtlvtest/test/datalevin/query/source_metadata_test.clj)
run with `lein test datalevin.query.source-metadata-test` from `../dtlvtest`.
They cover symbol/tuple metadata, eager join and aggregate parity, legacy
compression, cached expansion limits, partial scans, and reservation cleanup.

The initial path supports positive patterns, the baseline scalar predicates,
scalar and flat tuple/collection/relation input bindings, projection, `:with`,
count/sum/avg/min/max, and final limit/offset. It pins native views and groups
aliases into one source session.
Earlier fragments become quota-bound join tables; the final fragment streams
through joins and residual predicates. On its first row, core builds hash
indexes for shared variables using the native engine's key wrappers. Index
containers and growth are reserved before allocation; keys borrow already
charged table values. Tables without shared variables remain cross products.
An empty required table stops construction of later tables and skips the final
stream. Complete source planning still precedes execution.
Probes clear their scratch references before releasing the streamed batch,
and query cleanup clears indexes. Blocking state fails at its memory
limit; this implementation does not spill, even with a positive disk allowance.
Core row scratch, distinct/grouping state, and result retention check cancellation
and remaining quota incrementally while sizing, including any charge multiplier.
Output checks result-row limits before traversal and result-byte limits during
it; the final reservation atomically checks all applicable ledger categories.
Explain reports the structural and quantitative diagnostics specified in
[§8](#8-caching-errors-and-explanation), including captured public version
identifiers, without provider labels, handles, bindings, or rows.

For example, `:in $s [?id ...]` and `:in $s [[?id ?label]]` become owned,
quota-bound local join tables. Core checks every input value against the native
and external pattern types before provider acquisition. Only scalar values,
including values destructured from a single tuple, enter SPI `:bindings`;
collection/relation inputs do not request a cursor per input row. Capture
checks cancellation within nested values as well as between rows. Sizing checks
the live shared retained-memory budget incrementally before visiting container
children; a final atomic reservation covers the charge before copying mutable
leaves. Copying also checks cancellation, including inside metadata.
Subsequent type and planning validation also checks cancellation within these
values and tuple components; cancellation is not converted into a type error.
The planner validates each response once, then checks cancellation while
searching for complete, nonoverlapping coverage under the replay constraints.
It memoizes failed coverage states, including covered predicates, under the
retained-memory quota and releases that workspace after selection. Equivalent
failed paths do not consume additional search attempts. The current embedded
planner permits 4096 distinct states before rejecting excessive complexity with
`:query-source/unsupported` in `:planning`.
Pattern and predicate constants are also captured as owned values, including
metadata, with their retained charge reserved before copying. This capture
precedes input iteration and all provider callbacks; each invocation captures
the caller's current values afresh.
Empty input relations open no execution cursors. Flat tuples accept `_` and
ignore trailing values, matching native bindings; nested destructuring remains
unsupported.
Dynamic `:limit` and `:offset` variables must be direct scalar `:in` bindings,
with positive and nonnegative integer values respectively, bounded by
`Long/MAX_VALUE`. The literal negative-limit convention remains unlimited;
negative dynamic limits reject, matching native execution.

Custom values implement
[`IQuerySourceValue`](../src/datalevin/query/source/value.clj) to supply their
complete nominal descriptor and conservative retained size. This opt-in does
not install a storage codec or alter equality.
The shared `owned-copy` helper in that namespace preserves collection/symbol
metadata and default sorted collection behavior, and detaches mutable leaves,
including leaves inside metadata. Custom comparators require the immutable
custom-value contract; ordinary collection copying does not retain comparator
closures. The fake source and local adapter use it after reserving their batch
charges.

Source joins and projection distinctness use the shared native query keys;
nonbinary values retain Java equality and scalar predicates use the native
engine's built-ins. Decimal scale is significant for
keys, including scalar and tuple input bindings. Native result sets preserve
those keys during iteration and spilling, so projected `1.0M` and `1.00M`
remain distinct inputs to aggregation on both execution paths.
Return maps (`:keys`, `:strs`, `:syms`) preserve the same distinctions.
Native indexed input probes compare decoded stored values before dropping
bound columns; storage normalization, such as floating zero, does not replace
the join's key comparison. Native/eager comparisons use the values actually
stored when a storage encoding normalizes its input.
`sum` and `avg` preflight each group's numeric values and reserve conservative
workspace for wide arithmetic before invoking the native reducer. The bound
includes decimal scale alignment (including the initial integer zero), requested
`*math-context*` precision, and ratio denominator growth. It uses stored numeric
sizes, without expanding decimal powers or strings during preflight. Scratch is
released after each reducer; wide results receive a retained charge before that
release and remain charged through query completion. Cancellation is checked
during preflight and again between reduction inputs. Ordinary numeric semantics
and errors are preserved within the budget; `count`, `min`, and `max` do not
reserve sum/average workspace.

Byte content equality applies to scalar/tuple keys and nested portable data,
including map keys and set elements. Unordered values retain entry/element
multiplicity when distinct host array objects contain the same bytes. Query
key indexes borrow already owned values instead of making canonical copies.
Temporary indexes used to compare unordered values reserve workspace before
allocation, check cancellation during traversal, and release it on every exit.
Native inline and Nippy giant decoding accept bytes under the existing bounds,
including compressed payloads and metadata. Public predicate equality preserves
Clojure numeric equivalence; key equality preserves decimal scale.
Native generic values containing bytes use attribute-bounded content scans
(or entity/attribute scans when the entity is bound), because serialization
order cannot safely bound logical equality. Typed scalar indexes remain usable.
Source tuple descriptors may include byte components; native tuple schemas keep
their existing component restrictions.

The [byte execution regressions](../../dtlvtest/test/datalevin/query/source_bytes_test.clj)
compare eager and source execution across copies, batch boundaries, flat inputs,
constants, native joins, EAV facts, aggregation, and spilled result membership.
The [shared key tests](../test/datalevin/query_key_test.clj) cover hash collisions,
unordered multiplicity, numeric/custom equality, scratch quotas, cancellation,
and restoration of the enclosing query budget. The companion local adapter
uses the same shared comparisons and tests owned byte query results.

Unsupported execution shapes reject explicitly: prepared execution,
function/rule/branch/negation clauses, ordering, pull, and nested input
destructuring. Mixed native queries require platform threads, a local DB with
neither a pending transaction overlay nor an active write transaction, and
supported scalar, typed tuple, generic data, or portable registered Nippy values
wherever the query binds or compares a value.

The [execution-context regressions](../../dtlvtest/test/datalevin/query/source_context_test.clj)
cover remote DBs against a live server, pending native overlays, active writers,
and (on JDK 21+) virtual threads through `d/q`, plan-only explain, and executed
explain, with both source input orders. Each rejects with
`:query-source/unsupported`, phase `:native-view`, and the native source/clause
location before acquiring a provider session. Ordinary native operations remain
usable after rejection. Source-only embedded queries can run on virtual threads.
The native-image `:execution-mode` guard is not an end-to-end native-image
conformance claim; that mode remains outside v1 as specified in §1.

An explicit native value wildcard such as `[?e :document/body _]` uses a
quota-bound scan of entity/attribute pairs without reading giant payloads,
decoding values, or invoking custom codecs. It can therefore test existence on
byte, custom, and otherwise unsupported generic attributes. Each matching
fact contributes a pair, preserving cardinality-many behavior until ordinary
projection/aggregation. A named value variable still requires the supported
decoding contract, even when that variable is not returned.

Native scans with an unbound entity use the indexed attribute's AVE prefix,
visiting only its datoms. Bound entities use EAV with both the entity key and
attribute duplicate range restricted. For `:db/noindex`, an unbound scan visits
entity keys and seeks that attribute's duplicates, checking cancellation between
entities even when the attribute has no facts. Missing attributes open no
cursors. Prefix bounds use the pinned schema's attribute ID without invoking
value codecs, and include all giant/custom variants. The unindexed fallback
still scales with the number of entities, since it has no AVE entries.

Native constants and scalar/flat-tuple input bindings also restrict value
candidates for booleans, longs/references, floats/doubles except NaN, UUIDs,
instants, strings, keywords, and symbols. Bounds use AVE keys or EAV duplicates,
with a reservation before encoding. Unrelated values never enter the decoder;
giants sharing the selected truncated prefix still do. Core retains logical
equality checks for those collisions and Java input-key semantics such as signed
zero. Other value types keep the attribute/entity scan; no user codec or generic
serialization is invoked to construct a bound.

Native entity constants and IDs supplied by scalar, flat tuple, collection, or
relation `:in` bindings restrict EAV cursor ranges before payload reads or
decoding. Collection/relation inputs use a quota-bound set of distinct IDs;
their original rows still join locally, preserving duplicates and correlations
with other columns. This also applies to value wildcards and unindexed
attributes. Unrelated entities cannot consume the decoding budget or trigger
unsupported-value errors; selected values retain their normal type checks and
quotas. Early stop prevents opening subsequent ranges, and executed explain
counts each opened range in `:native-scans`.

When a source fragment supplies an otherwise unbound native entity variable
and the native value has no constant or scalar-input constraint,
the executor retains the source rows under quota before reading that native
pattern. It propagates this order through native reference chains. Each such
native reader borrows candidate IDs from a retained relation, deduplicates them
under quota, and visits only those entity ranges in the pinned view. Null and
non-Long candidates cannot match native entity keys and open no ranges. The
original rows still participate in core joins, preserving correlations and
`:with`; other join predicates remain core checks. Explicit entity bindings and
constant/scalar value constraints keep their earlier native reads, so an empty
native filter can still skip source execution. This is bounded local join
ordering, with no correlated provider calls or new SPI capability. The source
build side must fit the retained-state budget; the final native reader streams
to the result window and stops before later ranges once it is satisfied.

Fixed tuple attributes use `:db/valueType :db.type/tuple` with `:db/tupleTypes`,
including types inferred for composite attributes. Their 2–255 scalar components
map positionally to the SPI tuple descriptor; native reference components map
to `:long`. Missing native components remain `nil`; external rows must still
satisfy their advertised element types.

Homogeneous tuples declared with `:db/tupleType` support 1–255 non-null scalar
components and use the SPI `:data` descriptor, since their arity varies. Joins
require an external `:data` column; the native schema still validates component
types and input bindings. Native reference components remain ordinary entity
IDs represented as `:long`. This adds no variable-arity SPI tuple descriptor.

Untyped attributes and untyped composites use the SPI `:data` descriptor; joins
require an external `:data` column. Generic data supports JVM integers (including
`Byte` and `Short`), characters, ratios, the scalar representations above,
the Java time values from [Section 3](#3-logical-values-and-clause-envelopes),
and nested vectors, maps/sets with hashing or the default sorted
comparator, and lists, including bounded portable metadata. Preflight limits
nesting to 64 levels and caches to 256 entries; repeated cached subtrees count
toward the decoded-value charge at every occurrence. Ratio component lengths
and sorted collection counts are checked before allocation. Malformed values,
including zero ratio denominators or incomparable sorted keys, report
`:query-source/invalid`. Other JVM objects and custom readers remain unsupported.

The supported Java time values use fixed-size Nippy layouts. Preflight validates
their payload lengths;
constructor range/overflow failures report `:query-source/invalid`. Native and
source execution compare the decoded logical values. Native inequality scans
on generic data retain predicates because serialized order does not implement
logical comparison, including for negative instants and durations.

Registered native types with the default `:payload :nippy` use that same portable
data contract and advertise the logical value as `:data`; their order-key type
does not determine the payload's logical type. Joins therefore require an
external `:data` column, and equality compares payload values, including when
their order keys collide. The pinned view supplies both accepted type metadata
and payloads. Reads validate stored references without copying order keys, then
reserve buffers, decompression, and layout-derived decoding space through row
handoff. They do not invoke registered order functions or user codecs. Types
with user-defined payload deserializers reject before source acquisition;
Nippy custom readers and nonportable payload values reject before decoding.

Inline and giant values reserve decoding memory before allocation, including
tuple containers. Giant reads bound stored bytes and Zstd expansion, then
validate tags, collection counts, tuple arity, and bounded caches before
invoking readers. Generic collections and giant homogeneous tuples receive an
additional layout-derived reservation before decoding, held through row handoff.
Stored metadata on symbols, fixed tuples, homogeneous tuples, and their symbol
components uses the same bounded preflight and additional reservation, including
cached occurrences. Metadata does not relax declared component types or permit
custom readers. Nippy giants preserve metadata; typed inline indexes continue
to omit it, matching existing native storage behavior.
Preflight checks cancellation during large traversals. Raw Nippy and legacy
uncompressed, LZ4, Snappy, Zstd, and LZMA2 envelopes are supported. Legacy
expansion and decoder scratch are reserved before copying or decompressing the
payload, including when nested inside the storage Zstd envelope; the expansion
reservation lasts through row handoff. Zstd accepts one frame with a declared
output size and no external dictionary. LZMA2 accepts one XZ stream; its
dictionary and decoder arrays are charged before allocation and released when
returned, including after partial construction failures. Concatenated frames or
streams and trailing bytes reject.
Reading arbitrary custom payloads, encryption, and custom envelopes still
rejects before value decoding; explicit value wildcards avoid
that read.
Combining one-shot fragments within a session rejects unless a complete
reopenable alternative is available.

The earlier v1 F1 baseline is complete. The revised
[F1 gates](../roadmap.md#f1-execution-checks) remain open for SPI v2, its
four-shape execution matrix, and native performance sign-off. Control construction,
plan/batch validation, and provisional size constants remain internal
implementation details.

## 10. SPI v2: revised four-shape contract

Status: specified implementation target, not implemented by this documentation
change. This section defines SPI version `2`. Sections 2–9 remain the executable
version `1` contract. V2 retains the protocol method signatures, logical values,
clause coverage, ownership, authorization, cancellation, quota ledger, and
error/redaction rules unless explicitly replaced below. Unknown unqualified
fields are invalid; namespaced annotations grant no capability. Native-only
queries retain §1.3's direct execution and performance requirements.

### 10.1 Version selection and compatibility

`-source-info` keeps the required top-level fields from §2. Its
`:pattern-contract` must be `{:kind :eav :attributes {...}}` under the
EAV descriptor contract: every exposed attribute declares its entity and value
types, including nominal reference types where applicable. The accepted mapping
defines entity identity, attribute names, conversions, and missing-value
semantics. The required `:logical-schema` in the v2 map exposes the accepted
logical guarantees defined below. Core consumes that compiled schema; mapping
compilation and the physical evidence supporting its guarantees remain
companion-owned.

Revised federation planning admits only EAV source descriptors, including legacy
providers used through its v1 compatibility path. A tuple source must acquire an
accepted entity/attribute mapping before joining this model; a `:tuple` pattern
contract raises `:query-source/unsupported` before acquisition. Existing Datalog
tuple/collection inputs remain valid without an `IQuerySource` wrapper. Logical
tuple types within keys or attribute values, required-input tuples, and returned
binding tuples remain supported; they do not change the source pattern contract.

A source advertising version `2` must additionally supply the recognized, immutable
`:datalevin.query.source/v2` map:

```clojure
{:spi-versions #{2}
 ;; Other required §2 identity and fingerprint fields remain.
 ;; :pattern-contract is the compiled EAV descriptor described above.
 :features #{}
 :required-features #{}
 :datalevin.query.source/v2
 {:features #{}
  :required-features #{}
  :logical-schema logical-schema
  :read-capabilities #{:snapshot :statement :best-effort}
  :scope {:id "scope:tenant-a" :kind :restricted
          :parameters {:tenant :tenant-a}}
  :completion {:domain :finite :extent :all-matches}}}
```

Top-level feature sets belong to v1; the nested feature sets belong to v2.
All fields shown inside the v2 map are required. A v2-only source uses empty
top-level feature sets. Optional features are negotiated within the selected
version; dependent access and the other contracts in this section are baseline
v2 envelopes, not optional feature flags. `:bulk-bindings` does not select v2.

Core captures descriptors, forms physical session groups (§10.3), and chooses
the highest version supported by core and every alias in each group. Negotiated
features are the intersection of core support and every member's selected-version
feature set; every member's required features must be a subset. Core validates
that version's envelope and required features before acquisition.
No common version raises `:query-source/version`; a missing/malformed v2 map
raises `:query-source/invalid`. Unsupported required features raise
`:query-source/unsupported`. A query option `:query-source {:spi-version 2}`
requires exactly that version for every external group; omission allows the
highest common version per group. Do not retry a rejected v2 request as v1.
The option is a positive integer; an unsupported requested version raises
`:query-source/version`.

A source may advertise `#{1 2}` only when it implements both contracts faithfully
for the same logical definition. The namespaced v2 map remains ignorable metadata
to v1 core; the selected `:spi-version` determines interpretation. A scan-only
provider may implement v2 with empty `:requires`; a lookup-only provider must
not invent a v1 scan. Each session reports the selected version and features
exactly, and every planning/open request carries them. Handles and sessions
never cross versions. Mixed versions in different physical groups are allowed
only through core's explicit v1 compatibility path:

- an EAV provider using v1 receives its original request, alternative, and batch
  envelopes, with v2-only limit/option fields omitted from provider requests;
- its one effective consistency request is the requirement for that group;
- it supplies only input-free alternatives and its session-wide one-shot rule;
- its unknown costs remain unknown under v2 planning, and no consumption-group
  independence or dependent access is inferred from annotations;
- its entity/value types and v1 no-fact absence semantics remain available, but
  cardinality and explicit entity/reference domains are unknown. Core must not
  recover guarantees by interpreting provider-specific type parameters or
  annotations, including an unselected v2 envelope.

Legacy rows/width may supply advisory cardinality hints. V1 elapsed fields do
not establish the complete v2 elapsed estimate; missing run/setup components
remain unknown rather than being filled with zero.

These rules permit a revised core to use a legacy provider; they do not enable
v2 in the current executor. Conformance must test v1-only, v2-only, and dual
providers, mixed groups, incompatible aliases, and failure before acquisition.

#### Logical schema guarantees

`:datalevin.query.source/v2 :logical-schema` is a required immutable map with
exactly the unqualified keys `:domains` and `:attributes`. The pattern contract
continues to supply entity/value types; this envelope adds logical guarantees
without requiring core to parse a provider's mapping language. For the orders
example in §2, `order-type` and `customer-type` are its complete custom type
descriptors, and `order-domain` and `customer-domain` are their accepted nominal
domain IDs:

```clojure
{:domains {order-domain {:entity-type order-type}
           customer-domain {:entity-type customer-type}}
 :attributes
 {:order/id {:entity-domain order-domain
             :cardinality :one :absence :no-fact :reference-domain nil}
  :order/amount {:entity-domain order-domain
                 :cardinality :one :absence :no-fact :reference-domain nil}
  :order/customer {:entity-domain order-domain
                   :cardinality :one :absence :no-fact
                   :reference-domain customer-domain}}}
```

Every field shown for a domain or attribute is required. Namespaced annotations
remain inert. `:domains` is a nonempty finite map keyed by non-nil, non-secret,
immutable portable IDs; core compares those IDs without interpreting their
provider-specific structure. Each value supplies a complete logical
`:entity-type`. IDs must distinguish unrelated identity domains; sharing an ID
across sources requires an explicitly accepted common identity contract. The
companion may use its accepted `[source-instance-id domain identity-epoch]` IDs.
Domain entries may include a
reference target whose attributes this source does not expose.

`:attributes` has exactly the same keys as the EAV pattern contract:

| Field | Accepted logical guarantee |
|---|---|
| `:entity-domain` | An ID in `:domains` identifying the attribute's entity domain. That domain's `:entity-type` must equal the pattern's `:entity-type`. |
| `:cardinality` | `:one`, `:many`, or `:unknown`. `:one` permits zero or one distinct logical value per entity; `:many` permits zero or more; `:unknown` supplies no bound. Distinctness uses query-key equality. |
| `:absence` | Exactly `:no-fact` in v2: an absent value contributes no EAV fact, including for `_` and unprojected values. Returned entity/value terms remain non-nullable. |
| `:reference-domain` | `nil` for an attribute without a declared reference guarantee, otherwise an ID in `:domains`. Its `:entity-type` must equal the attribute's `:value-type`; it identifies the target domain of returned references. |

Domain compatibility uses the opaque ID together with its complete entity type,
including the type's version, fingerprint, and parameters under §3. Equal IDs
with incompatible types do not license shared-domain reasoning. Matching scalar
key types or attribute names cannot establish a shared domain. A declared
reference does not promise target existence, inverse uniqueness, or referential
integrity. Ordinary scalar-key joins keep their existing equality semantics.

These guarantees apply to the captured logical definition and accepted scope
under the acquired read context. `:one` is an at-most-one value guarantee; it
does not imply attribute presence, value-to-entity uniqueness, a physical row
count, or permission to change `:with` multiplicity. Aliases or mappings with
matching domains do not automatically share the same population or read view.
A combined mapped source must establish guarantees for its combined attribute
relations: two individually single-valued mappings can introduce conflicting
values for the same entity, so their union cannot simply inherit `:one`.

Core may use the accepted guarantees for reference-domain propagation,
single-valued access/fan-out reasoning, join planning, and validation of
semantic rewrites. It must retain an attribute's existence test when no separate
exact proof establishes presence. Such a proof may come from validated clause
coverage; cardinality, estimates, and reference declarations cannot supply it.
Rewrites comparing multiple observations must satisfy the read/replay contract;
one value per observation does not make separate weak reads identical.
Failed or incomplete requests cannot be interpreted as absent attributes.

The native bridge projects native attribute types/cardinality, native reference
domains, and no-fact absence semantics into the same planning vocabulary for
mixed queries. Native domains are qualified by database identity; this metadata
does not change native entity-ID representations or equality. Native-only
queries retain §1.3's direct path without allocating federation descriptors.

Core validates complete attribute coverage, enum values, domain references, and
type agreement before acquisition; violations raise `:query-source/invalid`.
It checks structure and compatibility, while providers establish and enforce
the claimed guarantees using accepted constraints, declared policies, or
validation. Physical columns, endpoint paths, conversion code, and proof
artifacts stay provider-owned. A known violated guarantee fails execution with
`:query-source/invalid`; it cannot silently become an estimate or be weakened
mid-query. Metadata validation does not prove the provider's physical data sound.

The complete logical schema participates in the definition fingerprint alongside
the EAV pattern contract and accepted scope. Changing cardinality, domains,
reference classification, or absence semantics requires a new accepted
descriptor and dependent-plan revalidation. Statistics and measured costs do
not change these guarantees. Plan-only explain may show their bounded logical
fields under §8's redaction and accounting rules; it must not expose supporting
physical metadata or proofs.

### 10.2 Accepted scope and completion

The v2 source descriptor fixes these contracts before planning:

| Field | Required meaning |
|---|---|
| `:scope :id` | Nonempty non-secret string identifying this accepted scope under the provider's versioned definition scheme. |
| `:scope :kind` | `:full` or `:restricted`, relative to the declared logical relation. Neither grants access beyond authorization. |
| `:scope :parameters` | Immutable portable map of accepted tenant, repository, partition, window, or other scope parameters; `{}` for an unrestricted scope. Credentials and runtime handles are forbidden. |
| `:completion :domain` | Exactly `:finite` in v2. Unbounded subscriptions/streams require another contract. |
| `:completion :extent` | `:all-matches` for the complete accepted relation, or `:declared-view` for an explicitly defined bounded/search/capped view. |

The complete scope and completion maps participate in the definition fingerprint,
along with the normalized mapping/query and conversions. The same scope ID must
not denote different accepted parameters. A `:declared-view` includes its exact
membership rule, cap, ordering/tie policy where relevant, and search/window
semantics in the accepted definition. It is a distinct logical relation, never
a way to advertise an incomplete scan of a broader one. Request bindings narrow
an access within that scope; they do not mutate it or enter its fingerprint.
Any scope change requires a new accepted descriptor and revalidation.

Each alternative names the captured `:scope-id` and promises
`:completion :all-bindings`. Successful cursor exhaustion means that every
supplied binding has received all matching rows for that alternative's exact
coverage within the accepted relation. Safe superset rows remain subject to
residual checks. Completion does not imply a snapshot across pages or opens;
the actual read guarantee in §10.3 applies.

Pagination tokens and retries remain provider-owned. A known result cap,
unavailable continuation, omitted page, or partial backend response must fail
with `:query-source/incomplete`, unless the narrower result is the accepted
`:declared-view` itself. Budget/deadline/cancellation failures retain their
existing categories. None may be returned as successful exhaustion. Reaching
the query's own early-stop condition closes the cursor without claiming source
completion; partial observations cannot back a complete replay spool.

### 10.3 Per-source read requirements and actual guarantees

Query options retain `:consistency` as a default and add alias overrides:

```clojure
{:query-source
 {:consistency :snapshot
  :sources {'$native {:consistency :snapshot}
            '$crm {:consistency :best-effort}}}}
```

The default is `:snapshot`. `:sources` keys must name actual source variables
in the expanded invocation, including native inputs in mixed queries; unknown
aliases/options are invalid. Each override has exactly `:consistency` (plus
inert namespaced annotations). These are minimum guarantees, ordered
`:snapshot > :statement > :best-effort`. A stronger actual guarantee is allowed
and reported; a weaker one is rejected. Native-only calls keep the native path.

Core groups aliases by source-instance ID, compute-context ID, and effective
principal/policy requirements, with compatible logical views. It takes the
strongest requirement across the group; it never opens separate weaker views
to bypass a conflict. The descriptors' `:read-capabilities` are nonempty sets
of the three guarantees; advertising a stronger guarantee also supports weaker
minima. If any member lacks a guarantee at least as strong as the effective
minimum, fail with `:query-source/unsupported` before acquisition. Different
physical groups keep independent requirements. Incompatible authorization or scope bindings
fail rather than being silently broadened or substituted.
Alias names or different requested consistency levels cannot by themselves
justify different compute-context IDs to evade this grouping rule.

V1 descriptors have no read-capability set. The compatibility path passes the
effective minimum through the original acquisition envelope and validates the
reported guarantee there; it cannot promise the v2 metadata-only rejection gate.

The common v2 context retains the seven fields in §4; `:consistency` is now the
group's effective minimum. Acquisition adds both `:source-descriptors` and
`:read-requirements`, a map from every alias in the group to its requested
minimum. Core forwards the selected common context unchanged to that group's
planning and opens. `-session-info` reports the original identity/authorization
fields, `:spi-version 2`, the negotiated features, and actual `:consistency`:

| Actual guarantee | Promise |
|---|---|
| `:snapshot` | All fragments, binding batches, pages, and permitted repeated reads in the group observe one pinned logical view. Loss of that view fails the query. |
| `:statement` | One cursor invocation, including all of its bindings, internal requests, and pages, observes one logical view. Different opens may observe different views. |
| `:best-effort` | The provider completes the declared access but cannot promise one stable view even within a cursor. |

A backend snapshot per HTTP request does not establish `:statement` for a
multi-page cursor. Acquisition may establish authorization and scope for a weak
session without acquiring a snapshot. All reported guarantees must satisfy the
effective minimum and be supported by every member's advertised capabilities
(including their implied weaker guarantees); otherwise close the session
and fail with `:query-source/unsupported`. Query explain records requested and
actual guarantees separately for every alias and the effective group minimum.

Session `:data-version` retains §4's meaning: an exact pinned group view token,
or `nil` if unavailable. Statement/best-effort sessions report `nil`; this
version introduces no per-cursor version-token envelope. Snapshot sessions may
also report `nil` when no external token exists. Read consistency, identical
replay, and exact version tokens remain independent declarations. A materialized
observation stabilizes those rows, not the backend history; it cannot upgrade
actual consistency or enable cross-read emptiness/uniqueness shortcuts.

### 10.4 Consumption groups and replay

V2 session information additionally contains `:consumption-groups`, a vector
of declarations, and session-wide request constraints:

```clojure
{:consumption-groups
 [{:id :orders-stream :opens :once :max-open-cursors 1}
  {:id :customer-lookups :opens :many :max-open-cursors 1}]
 :request-constraints
 {:max-concurrent-requests 2
  :rate-limit {:requests 10 :per-ms 1000}}}
```

Group IDs are unique non-secret keywords/strings within a session and last
until session close; their identity is `(session-id, group-id)`, not an alias,
alternative ID, or cursor. Every alternative names one existing group. All
paths/aliases consuming the same underlying one-shot resource must name the
same group. Independent streams may use separate groups in one session.
Provider-private resources shared across queries require provider-side ownership
enforcement as well; new query IDs cannot restore a consumed stream.

`:opens :once` permits one cursor-open attempt across all group alternatives,
aliases, and binding batches during the session, with `:max-open-cursors 1`.
`:opens :many` permits repeated opens up to the positive concurrent-open limit.
Core reserves the permission before calling open. A failed open spends a
one-shot permission; cleanup never replenishes it. Metadata/planning consumes
no permission. Internal page fetches/retries use the same cursor but consume
actual request budgets. No additional cursor is opened speculatively.

Each alternative's `:properties :replay` is `:identical` or `:unstable`.
`:identical` guarantees the same logical row multiset for the same alternative,
scope, and typed binding values in that session, allowing binding-ID renaming;
order remains unspecified. `:unstable` makes no such promise. This property
does not grant another open, override the group's limit, or upgrade consistency.
Repeated live API calls normally have unstable replay. Changed bindings describe
a different access and are not a replay of previous results.

A plan requiring repeated logical use must either have permitted identical
replay or explicitly materialize one complete observation under memory/spill
quotas. Spool identity includes the session/group, captured definition/scope,
exact coverage, and typed binding keys. Share it only when its complete rows
are sufficient for the other use through exact local evaluation. Closing an
incomplete cursor, observing a prefix, or recording one key does not complete
another key or relation. Spooling never grants a second one-shot open.

One-shot dependent access therefore needs all required keys in one legal input
batch, or a different complete access that core can spool and index. A plan
needing multiple opens of that group is rejected before execution. Unknown key
counts may be collected under quotas before its single open; exceeding the
admitted batch fails rather than truncating or reopening. A chosen full scan
may feed multiple local lookups only after its required complete spool exists.
Identical-replay reuse is forbidden for unstable accesses unless the plan
explicitly shares a captured complete observation; explain exposes that choice.

Session request constraints apply across all groups and aliases.
`:max-concurrent-requests` is positive; `:rate-limit` is either `nil` (no
additional advertised rate limit) or positive `:requests` and `:per-ms`, meaning
at most that many request starts in any rolling interval. Providers enforce
these alongside query concurrency limits and any limits shared across queries.
Every page, retry, and split request charges the cumulative request ledger;
waiting for a permit is cancellable and observes the query deadline.

### 10.5 Dependent access and binding batches

V2 alternatives retain `:id`, `:plan`, `:covers`, `:prefilters`, `:columns`, and
`:explain`. Their IDs are stable nonempty keywords/strings for an unchanged
structural planning request. They additionally require the following fields;
the v1 `:estimate` field moves to the conditioned estimate response in §10.6:

```clojure
{:requires [{:variable ?customer :type :long :nullable? false}]
 :scope-id "scope:tenant-a"
 :completion :all-bindings
 :properties {:consumption-group :customer-lookups
              :replay :unstable :ordering [] :blocking-state :none}
 :constraints {:max-binding-rows 100
               :max-binding-bytes 65536
               :max-request-binding-rows 20}}
```

`:requires` is an ordered vector of unique variable/type/nullability columns;
`[]` means input-free access. Every required variable must occur in the accepted
clause/input envelope and be available from query inputs or earlier operators
before open. No hidden correlation or provider callback into the outer query is
allowed. `:columns` declares returned variables; together they must supply the
variables needed to implement covered clauses and downstream demand. A required
variable may also be returned, but each returned value must equal its associated
input under `QueryKey.equals`. Repeated-variable types/nullability follow §5.

Core constructs a dependency DAG and rejects absent inputs, self-supplying
alternatives, and unseeded cycles before opening execution cursors. A constant
or a finite query/native/source input can seed a path. The admitted input domain
must satisfy every required column's type and nullability. Core cannot silently
drop unsupported keys: it must prove they cannot match the logical relation,
choose another complete path, or reject the plan. Runtime violations are errors.

Core deduplicates the ordered input tuples with shared `QueryKey.equals`/`hash`,
owns their mutable values, and retains the association to every outer tuple and
its multiplicity under quotas. It does not use raw-tuple Clojure map equality.
Deduplication is within the chosen logical access; unstable reads do not permit
an unannounced cache across separate access occurrences. Matches fan back out
before projection, `:with`, distinct, or aggregation. An empty input relation
produces no matches and opens no cursor.
Evaluate each distinct key once per logical access: either collect its outer
associations before lookup or retain its matches until all matching outer rows
have been processed. Exceeding retention quotas fails; it cannot silently turn
deduplication into repeated unstable reads.

V2 query limits add positive `:binding-rows` (default `256`) and `:binding-bytes`
(default `262144`). These cap each open's input batch, independently of returned
`:batch-rows`/`:batch-bytes`. Alternative constraint values are positive integers.
The effective cap is the minimum of provider and query row/byte caps. Core
splits by both; a single oversized key fails `:query-source/limit`. The provider
may split a legal cursor input further at `:max-request-binding-rows`; `1`
supports endpoints without bulk lookup. Pages and retries are additional actual
requests, never hidden in one logical-batch charge.

`-open-cursor` receives the seven common context fields, the four fields from
§6, and exactly these additions (besides inert namespaced annotations):

```clojure
{:scope-id "scope:tenant-a"
 :consumption-group :customer-lookups
 :bindings [{:binding-id 7 :values [42]}
            {:binding-id 8 :values [99]}]}
```

IDs are core-assigned query-local nonnegative signed-64-bit integers, opaque to
providers. An ID is never reused for a different logical access or key within
the query; exact replay may reuse its IDs. Each open has unique IDs and unique
input tuples under query-key equality. IDs persist through the access's
replay/spool and are not reassigned to different values. Values
are owned fixed-arity vectors in `:requires` order. Input-free access receives
one synthetic binding with `:values []` and its own ID. An empty binding vector
is not a legal open. Providers borrow these immutable-for-use values until
cursor close and copy them if retained longer, with retained-state accounting.

Before open, core validates IDs, arity, types/nullability, scope/fingerprint,
handle/session/group membership, binding caps, and available group permission.
The provider verifies the same captured execution contract. The handle contains
the immutable clauses and known planning constants; runtime bindings are exactly
the declared required inputs, not permission to replan or change coverage.

Returned owned batches keep §6's fields and add:

```clojure
{:binding-ids [8 7 7]
 :completed-binding-ids []
 :exhausted? false}
```

`:binding-ids` is parallel to `:rows`, with one requested ID per row; response
order and row values cannot substitute for it. Multiple rows for one ID and
identical rows for different IDs are legal. Unknown IDs, wrong lengths, or
incompatible returned input variables are invalid. Provider rows retain their
logical multiplicity; core does not deduplicate them merely by binding ID.

`:completed-binding-ids` must be empty on non-final batches. On the final batch,
`:exhausted? true` requires exactly all requested IDs, each once, including keys
with no matches. Final rows are allowed. This is the all-bindings completion
acknowledgment from §10.2; there is no partial-success result in ordinary `d/q`.
Empty non-final batches and `nil` exhaustion remain invalid. No subsequent
advance is allowed after the validated final batch.

V2 adds public sizing helpers `(binding-charge-bytes requires bindings)` and
`(batch-charge-bytes columns rows binding-ids completed-binding-ids)`. They use
the same conservative, cancellation-aware ownership accounting as §6, including
ID vectors and retained backing. Input batches are charged to retained memory
until their users release them; returned `:byte-count`/`:reservation` cover the
extended batch. Core sizes a legal final acknowledgment when splitting input
batches, so its IDs fit the output batch byte cap. Headers, dedup indexes, and
outer associations cannot escape accounting even when zero rows are returned.

### 10.6 Conditioned planning and deterministic selection

V2 uses two explicit phases of `-plan-source`; neither performs source I/O or
opens cursors. All planning metadata is bounded and charged as in §5. The common
context uses the acquired group's effective request, with actual guarantees
available in its validated session information.

**Alternatives.** One call per source symbol extends §5's request with
`:planning-phase :alternatives`, the captured `:scope-id`, and
`:available-inputs`, an ordered vector of typed variable columns that query
inputs or other operators could supply. It is an availability description, not
a promise that every dependency schedule is possible. `:bindings` still holds
only known scalar values. Return `{:alternatives [...]}` with §10.5 alternatives;
planning cannot consume outer relations to discover keys or selectivity.

**Estimates.** After constructing candidate schedules, core may call the same
method with `:planning-phase :estimates`. The request has the common context,
`:source-symbol`, `:source-descriptor`, `:scope-id`, and `:contexts` only. Each
context refers to an already returned alternative and is immutable:

```clojure
{:id 12
 :alternative-id :lookup-customers
 :input-rows 1200
 :distinct-keys 300
 :binding-count 100
 :binding-bytes 16384
 :invocations 3
 :request-concurrency 2
 :input-confidence :medium
 :known-bindings {?minimum 50}}
```

Context IDs are unique core-assigned query-local nonnegative signed-64-bit
integers. `:input-rows` is the
outer cardinality before key deduplication; `:distinct-keys` is for the ordered
`:requires` tuple, and `:invocations` is the number of opens represented by this
scenario. These counts and `:binding-bytes` are nonnegative finite estimates or
`nil`, never an assumed one/zero. `:binding-count` is a positive integer hypothetical
batch size within all known row caps, not an assertion that its keys exist.
Core uses separate contexts for full/tail batches when their sizes differ.
Input-free access has one empty binding per invocation. Concurrency is a
positive integer request count bounded by session/query constraints, not a
license to exceed rate limits. `:known-bindings` contains only previously
available scalar constants; there are no samples of unknown outer values.
`:input-confidence` is `:high`, `:medium`, `:low`, or `:unknown` and travels into
the whole-plan uncertainty calculation. Unknown byte fit still needs runtime
batch splitting; it cannot prove an invocation count.

Return `{:estimates [...]}`, one entry per requested context, with exactly:
`:context-id`, `:alternative-id`, `:cost`, `:confidence`, and `:provenance`.
No alternatives, handles, or new capability claims may appear in this response.
The context reference supplies the conditioning; a response cannot change it.
All keys in the following `:cost` table are required. Each value is a finite
nonnegative number or `nil` for unknown, per one cursor invocation at that
context's binding count and request concurrency:

| Cost field | Unit and inclusion rule |
|---|---|
| `:output-rows` | Returned rows across all supplied binding IDs, before core fan-out and residual checks. |
| `:row-bytes` | Mean owned logical row/container charge; excludes wire bytes and the separately sized binding-ID envelope. |
| `:read-rows`, `:read-bytes`, `:read-partitions` | Physical backend work examined, not merely rows returned. Unknown internal work is `nil`. |
| `:external-requests` | All expected backend operations/pages, parameter splitting, and retries for this invocation. In-process work may be zero. |
| `:transfer-bytes` | External bytes received, using wire/compressed size; zero for wholly local access. |
| `:startup-ns` | Cursor setup before row-processing work, excluding session acquisition. |
| `:run-ns` | Remaining elapsed time to completion, including fetches, pagination, decoding/owned-copy work, retries, rate-limit waits, and provider spill at the stated concurrency. |
| `:peak-retained-bytes` | Peak provider-owned live state during this invocation, distinct from transferred batches/core state. |
| `:spill-read-bytes`, `:spill-write-bytes` | Expected provider spill I/O; its elapsed time is already included above. |

Envelope `:confidence` describes the non-`nil` estimates, using the same four
levels as input confidence; an entirely unknown estimate uses `:unknown` and
all cost fields `nil`. `:provenance` requires `:kind` (one of `:exact`,
`:statistics`, `:heuristic`, `:unknown`), `:id` (non-secret string or `nil`),
`:observed-at-ms` (epoch milliseconds or `nil`), and `:valid-for-ms` (duration
in milliseconds or `nil`).
Timestamps/durations are nonnegative integers. Statistics with missing/future
timestamps or expired validity are stale for ranking. A static exact/heuristic
model may use `nil` timestamps. No SQL, raw keys, samples, or secrets belong in
provenance. Estimated zero is never proof of emptiness or zero resource use.

Core may refine downstream contexts after receiving upstream cardinality
estimates, within these query options and fixed defaults:

```clojure
{:query-source
 {:planning {:max-alternatives 32
             :max-plan-states 1024
             :max-estimate-contexts 128
             :max-estimate-rounds 4
             :estimate-max-age-ms 300000}}}
```

The first bound is per source enumeration call; plan states, estimate contexts,
and rounds are query-wide totals. All are positive integers except nonnegative
maximum age. A round calls each source at most once with its pending contexts.
Enumerate unique partial/complete plan states breadth-first by placed-fragment
count and then canonical plan key, counting each expanded state once. Use
canonical source/alternative/context order for estimate rounds. Failure to find
a complete admissible plan within the budget raises
`:query-source/unsupported` with reason `:planning-budget`; it does not execute
a partial plan. With a complete plan available, exhausted estimate budgets
leave unresolved costs unknown and use the fallback below. Explain reports
search/estimate budget exhaustion; there is no hidden unbounded optimizer pass.

Core validates complete coverage, typed dependencies, scope/completion, actual
read guarantees, consumption permissions, and hard provider constraints before
ranking. It chooses batch sizes under both byte and row caps; known request/rate
constraints bound scheduling independently of estimates. Unknown cardinality
does not bypass quotas, and no estimate reserves resources or certifies fit.
Providers may return unknown costs but must provide enforceable capabilities
and constraints. Provider-local optimizer scores and billing units are not costs
in this envelope.

The initial ranking policy is versioned `:latency-v1`. It models a serial core
fragment schedule and does not credit overlap across fragment invocations.
Sum `startup-ns + run-ns` for each estimated invocation, with separate full/tail
contexts, and add calibrated core operator time for key deduplication, fan-out,
joins, residuals, blocking operators, and spools. Count legal shared/fused work
once; multiply dependent work by the modeled invocation count, not by outer
multiplicity. Provider-internal concurrency is already reflected in `:run-ns`;
do not add request latency, transfer time, decode time, or spill time a second
time. Session acquisition is measured once per group and added as a common
query cost, never charged per alternative. Retained/spill state follows the
operator lifetime schedule rather than adding peak estimates as elapsed time.
Missing core calibration, invocation counts, or elapsed operands makes the
whole-plan elapsed estimate unknown.

Use latency ranking only when every remaining complete candidate has a finite
elapsed estimate, all inputs/time operands have at least medium confidence,
and statistics are fresh at core's single captured planning timestamp. Freshness
uses the smaller of provider validity and `:estimate-max-age-ms`; missing
statistics validity is stale. Low, unknown, or stale evidence remains visible
but cannot discard a competing complete plan. Otherwise apply one structural
fallback to all complete candidates, lexicographically minimizing:

1. explicit replay spools;
2. blocking core materializations;
3. dependency depth (longest required-input path in the operator DAG);
4. dependent-access sites;
5. physical fragments; and
6. the canonical plan key.

These are structural counts, not estimates with unknowns replaced by zero.
For equal latency scores use the same tie-break tuple. The canonical key encodes
source symbols, stable alternative IDs, covered clause IDs, required-variable
order, and core operator topology in a fixed tagged order. Compare keywords by
namespace/name, symbols by their full qualified name, and strings by Unicode
code-point order, with explicit type tags distinguishing keywords from strings.
It excludes handles,
session/query UUIDs, map iteration order, provider labels, and workload labels.
Explain records `:policy :latency-v1`, `:selection` (`:estimated` or
`:structural-fallback`), `:estimated-elapsed-ns`, and `:planning-time-ms`.
`:fallback-reasons` is a sorted vector drawn from `:unknown-cost`,
`:low-confidence`, `:stale-statistics`, `:estimate-budget`, and
`:uncalibrated-core`, empty for estimated selection. It also reports
conditioning/confidence/freshness and separate actual metrics;
raw binding values stay redacted. The same contracts, planning inputs, and
captured timestamp must yield the same choice regardless of response ordering.

### 10.7 Revision conformance and failure gates

V2 remains unavailable until core and companion tests cover the revised
envelopes and the four shapes, including these requirements:

- mapped native/SQL/file/API entities and attributes share source-qualified
  EAV query semantics, including nominal identity, references, and missing
  attributes; tuple source descriptors reject before acquisition, including
  through the v1 compatibility path, while ordinary Datalog tuple inputs and
  binding/result batches remain valid;
- logical-schema validation rejects missing attributes, unresolved domains,
  incompatible entity/reference types, and invalid cardinality/absence policies
  before acquisition; v1 compatibility leaves unavailable guarantees unknown;
- native and external fixtures expose equivalent accepted logical guarantees;
  absent single-valued attributes, dangling references, shared-domain mappings
  with different populations, combined mappings with conflicting values, and
  weak reads cannot justify unsafe existence, uniqueness, or replay rewrites;
- logical-guarantee changes invalidate fingerprints and dependent plans while
  statistics changes do not; provider proof artifacts remain outside public
  descriptors and diagnostics;
- negotiation never dispatches v2 fields through v1, never downgrades after an
  error, and rejects incompatible shared-alias versions before acquisition;
- required-input DAGs, empty seeds, unseeded cycles, binary/decimal key identity,
  duplicate outer multiplicities, nullable inputs, input-free synthetic IDs,
  arbitrary response order, zero-match IDs, malformed IDs, and row/byte splitting;
- bulk and per-key endpoints charge every request/page/retry, respect shared
  concurrency/rate limits, and clean up after failure between binding batches;
- independent source guarantees coexist, shared aliases use the strongest
  minimum, unsupported conflicts fail, and multi-page weak reads never claim
  statement or snapshot consistency;
- independent one-shot groups each open once, aliases of the same stream cannot
  reopen it, failed opens consume permission, and bounded complete spools support
  only the bindings/coverage actually captured;
- final completion includes every requested ID, even for no matches; caps,
  missing pages, budget exhaustion, and early stops never masquerade as complete
  broader relations or reusable complete spools;
- cheap invalid alternatives lose before ranking; known selectivity, key counts,
  batch ceilings, latency, transfer, and rate limits affect comparable costs;
  unknown/stale/missing estimates trigger the specified structural fallback;
- ties, reordered provider responses, estimate-budget exhaustion, and OLTP/OLAP
  relabeling preserve deterministic selection; inaccurate estimates affect
  performance but never result correctness, completeness, or quota enforcement;
- revised execution retains native-only routing and the mandatory native
  performance gate. No v2 bridge is activated merely to validate native queries.

Malformed envelopes use `:query-source/invalid`; no legal capability/dependency
schedule uses `:query-source/unsupported`; known incomplete access uses
`:query-source/incomplete`. Existing authorization, stale-definition, lost-view,
limit, cancellation, timeout, provider-failure, and cleanup categories retain
their meanings. All new errors and explain data follow §8's redaction rules.
