# Datalevin Command Line Tool

`dtlv` is a native command line tool for Datalevin. It can work as an
interactive console, a \*nix shell command, a Babashka pod, or a server.

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
so basic Clojure programming is supported, e.g.

```console
user> (+ 1 2 3 4)
10
user>
```

As a REPL for Datalevin, all Datalevin [public
functions](https://juji-io.github.io/datalevin/index.html) in the
`datalevin.core` namespace can be directly used, without needing to require
the namespace.

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

user> (def conn (get-conn "/tmp/test-db"))
#'user/conn
user> (transact! conn [{:greeting "hello"}])
{:datoms-transacted 1}
user>
```

## Shell Command

`dtlv` can be used to execute queries and conduct transactions in shell scripting.

`dtlv exec` executes the followed text as code.

```console
$ dtlv exec '(def conn (get-conn "/tmp/test-db")) \
> (q (quote [:find ?e ?n :where [?e :name ?n]]) @conn) \
> (close conn)'
```

If no code is given, `dtlv exec` takes the code from the standard input.  For example,
Here Doc is used here to give input:

```console
$ dtlv exec << EOF
> (def conn (get-conn "/tmp/test-db"))
> (q '[:find ?g :where [__ :name ?g]] @conn)
> EOF
#'user/conn
#{["hello"]}
```

## Babashka Pod

See [here](https://github.com/juji-io/datalevin#babashka-pod)

## Server

`dtlv serv` runs in server mode and accepts connection on port 8898 (default). Use option
`-p` to specify an alternative port number that the server listens to.

When a client opens a Datalevin database using a connection URI, i.e.
"dtlv://&lt;username&gt;:&lt;password&gt;@&lt;hostname&gt;:&lt;port&gt;/&lt;db-name&gt;",
instead of a local path name, a connection to the server is attempted. When the
connection is successfully established, the returned object contains a
connection token. The subsequent calls pass along the connection token.

The transportation between a client and the server uses
[transit+json](https://github.com/cognitect/transit-format) encoded strings.
