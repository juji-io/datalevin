# Datalevin Inference Engine

Datalevin has a production rule engine.

## Usage

## Implementation

Our forward-chaining inference algorithm is based on the fixed-point iteration
principle, i.e. inference terminates when an iteration of rule application
derives no triples.  Compared with RETE algorithm, this approach is simpler and
has smaller memory footprint. It is a better fit for a database system equipped
with a performant query engine.

Before performing joins, the inference engine use a specialized component to
compute transitive closure using the schema [1].

## Reference

[1] Subercaze, J., et al. "Inferray: fast in-memory RDF inference." VLDB.
Vol. 9. 2016.
