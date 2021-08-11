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
`-p` to specify an alternative port number that the server listens to. `-r`
option can be used to specify a root directory path on the server, where data
live. The default is `/var/lib/datalevin`.

When run locally, the default root user `datalevin` is assumed, and no need to
specify the default password (also `datalevin`). For remote access, username and
password is required. REPL commands can be used to change password, add users, etc.

When a client opens a Datalevin database using a connection URI, i.e.
"dtlv://&lt;username&gt;:&lt;password&gt;@&lt;hostname&gt;:&lt;port&gt;/&lt;db-name&gt;?store=datalog|kv",
instead of a local path name, a connection to the server is attempted. So
the same functions for local databases work on the remote databases. The remote
access is transparent to callers.

The wire protocol between server and client uses TLV message format (1 byte type + 4 bytes length +
payload value bytes). For example, with type `1`,
[transit+json](https://github.com/cognitect/transit-format) encoded bytes will
be the payload format. Other format will be added in near future, e.g. nippy if
only Clojure clients are expected.

The server uses non-blocking event driven architecture, so it support a large
number of concurrent connected clients. A work stealing thread pool is used to handle
individual incoming requests.

For programming convenience,  the client uses blocking network connections
managed by a connection pool.
