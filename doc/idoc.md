# Datalevin Idoc (Indexed Documents)

Datalevin provides document database feature through idoc. Idoc is a
structured-document value type with a built-in index. The index is keyed on
paths in the document. It lets you store nested documents in regular datoms
while supporting fast path/value matching in Datalog queries.

Idoc is designed to be orthogonal to full-text and vector indices. You can index
the same document in multiple ways (idoc, fulltext, vector) and mix their
queries in a single Datalog query.

Another common use of path indexed documents is to have flexibility in data
modeling, as the document format can evolve independently without touching the
schema.

## Usage

### Schema

Declare idoc attributes with `:db/valueType :db.type/idoc`. You can choose an
optional idoc format and a domain name:

* `:db/idocFormat` -- one of `:edn` (default), `:json`, or `:markdown`.
* `:db/domain` -- optional idoc domain. If absent, the attribute name is used as
  the domain. If specified, the values of this attribute will be added to the
  domain. Domain allows scoped search of idocs.

```clojure
(def schema
  {:doc/edn  {:db/valueType  :db.type/idoc
              :db/domain     "profiles"}
   :doc/json {:db/valueType  :db.type/idoc
              :db/idocFormat :json}
   :doc/md   {:db/valueType  :db.type/idoc
              :db/idocFormat :markdown}})
```

### Transact idoc values

Idoc values must be maps whose keys are keywords or strings, i.e. top level of
the document must be a map. Vectors are allowed as arrays. You can nest map and
vectors arbitrarily. However, the maximal path when binary encoded cannot be
over 511 bytes.

lists are **not** allowed. `nil` values are normalized to `:json/null`. Literal
`:json/null` is reserved and cannot be used in input. For `:edn` format, strings
are parsed with `clojure.edn/read-string` and must yield a map.

```clojure
(d/transact! conn
  [{:db/id   1
    :doc/edn {:status  "active"
              :profile {:age 30 :name "Alice"}
              :tags    ["a" "b" "b"]}
    :doc/json "{\"name\":\"Alice\",\"middle\":null,\"age\":30}"
    :doc/md   "# User Profile\n## Getting Started!\nName: Alice\nAge: 30"}])
```

`:doc/md` uses Markdown parsing (see Implementation below), producing a nested
map; the parsed map is stored in the datom and indexed.

### Document requirements

Idoc enforces a few structural rules at ingest:

* Top-level value must be a map (no top-level vector or scalar).
* Keys must be keywords or strings.
* Lists are not allowed anywhere; use vectors for arrays.
* `nil` values are normalized to `:json/null`.
* Literal `:json/null` is reserved and rejected on input.
* EDN strings must parse to a map.
* Markdown content must start under a header (text before any header is invalid).
* Circular references are rejected.

### Query with `idoc-match`

`idoc-match` is a Datalog query function that returns matching datoms as `[e a
v]` triples.

* Full DB search: `[(idoc-match $ {:status "active"}) [[?e ?a ?v]]]`
* Attribute specific: `[(idoc-match $ :doc/edn {:status "active"}) ...]`
* Domain specific: `[(idoc-match $ {:status "active"} {:domains ["profiles"]}) ...]`

```clojure
;; attribute-specific search
(d/q '[:find ?e ?a
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     {:status "active" :profile {:age 30}})
```

#### Nested maps and arrays

Map values can be nested maps. Vectors are treated as arrays: a match succeeds
if **any** array element matches.

```clojure
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn {:tags "b"})
               [[?e ?a ?v]]]]
     db)
;; => #{[1]}
```

#### Logical combinators

Boolean expressions can be used to combine match conditions. Use `[:and ...]`,
`[:or ...]`, and `[:not ...]` inside a query map:

```clojure
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn
                            {:profile [:or {:age 30} {:age 40}]})
               [[?e ?a ?v]]]]
     db)
```

#### Predicates

Predicates can be used as map values or as a standalone expression with a path.
Supported predicates are `nil?`, `>`, `>=`, `<`, and `<=`. Comparison operators
support multiple arity, so you can express ranges without a dedicated
`between` predicate.

```clojure
;; inline predicate in a map value
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn {:age (> 21)})
               [[?e ?a ?v]]]]
     db)

;; path predicate (quote the list when passed as data)
(d/q '[:find ?e
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     '(>= [:profile :age] 30))

;; range predicate via multi-arity comparison
(d/q '[:find ?e
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     '(< 20 [:profile :age] 40))
```

#### Wildcard paths

Wildcard segments can be used in path expressions and map keys:

* `:?` matches exactly one path segment.
* `:*` matches any depth (zero or more segments).

```clojure
;; any single key under :profile with value >= 30
(d/q '[:find ?e
       :in $ ?q
       :where [(idoc-match $ :doc/edn ?q) [[?e ?a ?v]]]]
     db
     '(>= [:profile :?] 30))

;; match any depth for a key
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/edn {:* {:product "B"}})
               [[?e ?a ?v]]]]
     db)
```

`:?` and `:*` are reserved as wildcard segments in queries.

`nil` is not a valid query value. To match nulls, use `(nil?)`:

```clojure
(d/q '[:find ?e
       :in $
       :where [(idoc-match $ :doc/json {"middle" (nil?)})
               [[?e ?a ?v]]]]
     db)
```

Note: JSON keys are strings. If the stored document comes from JSON, match with
string keys (e.g. `"middle"`), not keywords.

### Extract values with `idoc-get`

`idoc-get` extracts a value by path from an idoc document. It returns a vector
when the path traverses arrays.

```clojure
(def doc (:doc/edn (d/entity db 1)))
(idoc-get doc :profile :age)     ;; => 30
(idoc-get doc :tags)             ;; => ["a" "b" "b"]
```

## Implementation details

* **Document storage**: The original document is stored in the datom value. The
  idoc index only stores references to the datom, similar to fulltext indexing.
* **Index structure** (per idoc domain):
  * **doc-ref map**: `datom-ref -> doc-id` (doc-ref is datom or a giant datom id).
  * **path dictionary**: `path -> path-id` with stable numeric ids.
  * **inverted index**: `(path-id, typed-value) -> [doc-id ...]`.
* **Indexing**: During transactions, idoc indices are updated synchronously.
  There is no multi-step indexing process.
* **Large values**: Values that exceed the index key size are indexed by a
  truncated prefix (same scheme used by core indices). This can introduce
  extra candidates, but exact matches are verified against the full document
  during query evaluation.
* **Id ranges**: Idoc assigns a per-domain document id using 32-bit signed
  integers (about 2.1 billion docs per domain). Path ids are also 32-bit
  integers and are append-only.
* **Paths**: Paths are encoded as strings with `/` separators and include
  keyword vs string segment markers for lossless decode.
* **Markdown**: Markdown is parsed into a nested map. Headers are normalized
  (lowercase, punctuation removed, whitespace to `-`). Text content under a
  header is stored as a string. The normalized header keys are indexed and
  matched during query.
* **JSON nulls**: JSON `null` values are normalized to `:json/null` at ingest.
  Querying for null requires `(nil?)`.

## Notes
