# Datalevin Command Line Tool

`dtlv` is a native command line tool for Datalevin. The single binary executable
can work as an interactive console, a \*nix shell command, a Babashka pod, or a
server.

```console

$ dtlv help

  Datalevin (version: 0.9.11)

Usage: dtlv [options] [command] [arguments]

Commands:
  copy  Copy a database, regardless of whether it is now in use
  drop  Drop or clear a database
  dump  Dump the content of a database to standard output
  exec  Execute database transactions or queries
  help  Show help messages
  load  Load data from standard input into a database
  repl  Enter an interactive shell
  serv  Run as a server
  stat  Display statistics of database

Options:
  -a, --all                            Include all of the sub-databases
  -c, --compact                        Compact while copying
  -d, --dir PATH                       Path to the database directory
  -D, --delete                         Delete the sub-database, not just empty it
  -f, --file PATH                      Path to the specified file
  -g, --datalog                        Dump/load as a Datalog database
  -h, --help                           Show usage
  -l, --list                           List the names of sub-databases instead of the content
  -p, --port PORT  8898                Listening port number
  -r, --root ROOT  /var/lib/datalevin  Server root data directory
  -v, --verbose                        Show verbose server debug log
  -V, --version                        Show Datalevin version and exit

Type 'dtlv help <command>' to read about a specific command.

```

## Interactive Console

Start `dtlv` without any arguments or options, or start with `dtlv repl` command
will initiate an interactive console (REPL).

```console
$ dtlv

  Datalevin (version: 0.9.11)

  Type (help) to see available functions. Some Clojure core functions are also available.
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

As a REPL for Datalevin, all Datalevin public
functions can be directly used, without
needing to require the namespace.

```console
user> (help)

In addition to some Clojure core functions, the following functions are available:

In namespace datalevin.core

add                   clear                 clear-dbi             close
close-db              close-kv              closed-kv?            closed?
conn-from-datoms      conn-from-db          conn?                 copy
create-conn           datom                 datom-a               datom-e
datom-v               datom?                datoms                db
db?                   dir                   drop-dbi              empty-db
entid                 entity                entity-db             entries
get-conn              get-first             get-range             get-some
get-value             index-range           init-db               k
list-dbis             listen!               open-dbi              open-kv
pull                  pull-many             put-buffer            q
range-count           range-filter          range-filter-count    read-buffer
reset-conn!           resolve-tempid        retract               rseek-datoms
schema                seek-datoms           stat                  tempid
touch                 transact              transact!             transact-async
transact-kv           unlisten!             update-schema         v
with-conn

In namespace datalevin.interpret

exec-code             inter-fn              inter-fn?

In namespace datalevin.client

assign-role           close-database        create-database       create-role
create-user           disconnect-client     drop-database         drop-role
drop-user             grant-permission      list-databases        list-databases-in-use
list-role-permissions list-roles            list-user-permissions list-user-roles
list-users            new-client            query-system          reset-password
revoke-permission     show-clients          withdraw-role

Can call function without namespace: (<function name> <arguments>)

Type (doc <function name>) to read documentation of the function

user> (def conn (get-conn "/tmp/test-db"))
#'user/conn
user> (transact! conn [{:greeting "hello"}])
{:datoms-transacted 1}
user>
```
We are unapologetic about the use of Clojure language. Unlike most other
database software, the same code for embedded Datalevin library works in the
Datalevin REPL. There's no need to learn a new language just for the command
line shell.

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
(def conn (get-conn "/tmp/test-db"))
(transact! conn [{:name "world"}])
(q '[:find ?g :where [_ :name ?g]] @conn)
(close conn)
EOF
```

`dtlv` is also the tool for database maintenance.
etc.

Use `dtlv copy` to backup databases, optionally compacting the database files.

```console
$ dtlv help copy

  Command copy - Copy the database. This can be done regardless of whether it is
  currently in use.

  Required option:
      -d --dir PATH   Path to the source database directory
  Optional option:
      -c --compact    Compact while copying. Only pages in use will be copied.
  Required argument:
      Path to the destination directory.

  Examples:
      dtlv -d /data/companydb -c copy /backup/companydb-2021-09-14
```

Use `dtlv dump` and `dtlv load` to export and import databases as text files.

```console
$ dtlv help dump

  Command dump - dump the content of the database or sub-database(s)

  Required option:
      -d --dir PATH   Path to the source database directory
  Optional options:
      -a --all        All of the sub-databases
      -f --file PATH  Write to the specified target file instead of stdout
      -g --datalog    Dump as a Datalog database
      -l --list       List the names of sub-databases instead of the content
  Optional arguments:
      Name(s) of sub-database(s)

  Examples:
      dtlv -d /data/companydb -l dump
      dtlv -d /data/companydb -g dump
      dtlv -d /data/companydb -f ~/sales-data dump sales
      dtlv -d /data/companydb -f ~/company-data -a dump

$ dtlv help load

  Command load - load data into the database or a sub-database.

  Required option:
      -d --dir  PATH  Path to the target database directory
  Optional option:
      -f --file PATH  Load from the specified source file instead of stdin
      -g --datalog    Load a Datalog database
  Optional argument:
      Name of the single sub-database to load the data into, useful when loading
      data into a sub-database with a name different from the original name

  Examples:
      dtlv -d /data/companydb -f ~/sales-data load new-sales
      dtlv -d /data/companydb -f ~/sales-data -g load
```

`dtlv drop` and `dtlv stat` exposes some functionalities of the underlying LMDB
databases. `dtlv drop` clear/delete LMDB sub-databases. `dtlv stat` show
statistics of LMDB sub-databases.

## Babashka Pod

See [here](https://github.com/juji-io/datalevin#babashka-pod)

## Server/client

See [here](https://github.com/juji-io/datalevin/blob/master/doc/server.md)
