# Datalevin Command Line Tool

`dtlv` is a native command line tool for Datalevin. It can work as an
interactive console, a *nix shell command, a Babashka pod, or a networked
server.

## Interactive Console

Start `dtlv` without any arguments or options, or start with `dtlv repl` will initiate an interactive console (REPL).

```console
$ dtlv

  Datalevin (version: 0.4.40)

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

Obviously, as a REPL for Datalevin, all Datalevin [public
functions](https://juji-io.github.io/datalevin/index.html) in the
`datalevin.core` namespace can be directly used, without needing to require
them.

```console
user> (help)
The following Datalevin functions are available:

clear-dbi           close               close-db            close-kv
closed-kv?          closed?             conn-from-datoms    conn-from-db
conn?               copy                create-conn         datom
datom-a             datom-e             datom-v             datom?
datoms              db                  db?                 dir
drop-dbi            empty-db            entid               entity
entity-db           entries             get-conn            get-first
get-range           get-some            get-value           index-range
init-db             k                   list-dbis           listen!
open-dbi            open-kv             pull                pull-many
put-buffer          q                   range-count         range-filter
range-filter-count  read-buffer         reset-conn!         resolve-tempid
rseek-datoms        schema              seek-datoms         stat
tempid              touch               transact            transact!
transact-async      transact-kv         unlisten!           update-schema
v                   with-conn

Call function just like in code: (<function> <args>)

Type (doc <function>) to read documentation of the function
```

## Shell Command

`dtlv exec` execute given text string as code. If no code is given, it takes
text from standard input. Therefore it can be used to execute queries and
conduct transactions in shell scripting.
