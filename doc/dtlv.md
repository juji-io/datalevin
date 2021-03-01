# Datalevin Command Line Tool

`dtlv` is a native command line tool for Datalevin.

## Interactive Console

Start `dtlv` without any arguments or options will initiate an interactive
console (REPL).

```console
$ dtlv

  Datalevin (version: 0.4.3)

  Type (help) to see available functions. Clojure core functions are also available.
  Type (exit) to exit.

user>
```
The REPL runs a [Simple Clojure Interpreter](https://github.com/borkdude/sci),
so Clojure core function can be used, e.g.

```console
user> (+ 1 2 3 4)
10
user>
```
Let us define a Datalog schema.

```console
user> (def schema {:aka  {:db/cardinality :db.cardinality/many}
                   :name {:db/valueType :db.type/string
                          :db/unique    :db.unique/identity}})
#'user/schema
user>
```

Obviously, as a REPL for Datalevin, all Datalevin [public
functions](https://juji-io.github.io/datalevin/index.html) in the
`datalevin.core` namespace can be directly used, without needing to require
them.

```console
user> (def conn (d/get-conn "/tmp/datalevin/mydb" schema))
#'user/conn
user>
```
