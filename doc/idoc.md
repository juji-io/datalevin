# Datalevin Idoc (Indexed Documents)

Idoc is a structured-document value type with a built-in index. It lets you
store nested documents in regular datoms while supporting fast path/value
matching in Datalog queries. Idoc complements full-text and vector search: you
can keep a canonical document in the DB, then add full-text, vector, and idoc
indices as needed.

## Usage

### Schema

Declare idoc attributes with `:db/valueType :db.type/idoc`. You can choose an
optional idoc format and a domain name:

* `:db/idocFormat` -- one of `:edn` (default), `:json`, or `:markdown`.
* `:db/domain` -- optional idoc domain. If absent, the attribute name is used as
  the domain. If specified, the values of this attribute will be added to the
  domain.

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

Idoc values must be maps whose keys are keywords or strings, i.e. top level must
be a map. Vectors are allowed as arrays. You can nest map and vectors
arbitrarily. However, the maximal path cannot be over 511 bytes.

lists are **not** allowed. `nil` values are normalized to `:json/null`. Literal
`:json/null` is reserved and cannot be used in input.

```clojure
(d/transact! conn
  [{:db/id   1
    :doc/edn {:status  "active"
              :profile {:age 30 :name "Alice"}
              :tags    ["a" "b" "b"]}
    :doc/json "{\"name\":\"Alice\",\"middle\":null,\"age\":30}"
    :doc/md   "# User Profile\n## Getting Started!\nName: Alice\nAge: 30"}])
```

`#doc/md` uses Markdown parsing (see Implementation below), producing a nested
map; the parsed map is stored in the datom and indexed.

### Document expectations

Idoc enforces a few structural rules at ingest:

* Top-level value must be a map (no top-level vector or scalar).
* Keys must be keywords or strings.
* Lists are not allowed anywhere; use vectors for arrays.
* `nil` values are normalized to `:json/null`.
* Literal `:json/null` is reserved and rejected on input.
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
Supported predicates are `nil?`, `>`, `>=`, `<`, `<=`, and `between`.

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
  * **doc-ref map**: `doc-id -> datom-ref` (datom or a giant pointer).
  * **path dictionary**: `path -> path-id` with stable numeric ids.
  * **inverted index**: `(path-id, typed-value) -> [doc-id ...]`.
* **Indexing**: During transactions, idoc indices are updated synchronously.
  There is no multi-step indexing process.
* **Paths**: Paths are encoded as strings with `/` separators and include
  keyword vs string segment markers for lossless decode.
* **Markdown**: Markdown is parsed into a nested map. Headers are normalized
  (lowercase, punctuation removed, whitespace to `-`). Text content under a
  header is stored as a string. The normalized header keys are indexed and
  matched during query.
* **JSON nulls**: JSON `null` values are normalized to `:json/null` at ingest.
  Querying for null requires `(nil?)`.

## Notes

Idoc is designed to be orthogonal to full-text and vector indices. You can
index the same document in multiple ways (idoc, fulltext, vector) and mix their
queries in a single Datalog query.
