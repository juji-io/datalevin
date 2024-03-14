# Installation

Datalevin can be installed with different methods, depending on how you plan to use it.

## Clojure Library

The core of Datalevin is a Clojure library, simply add it to your project as a dependency
and start using it!

If you use [Leiningen](https://leiningen.org/) build tool, add this to the
`:dependencies` section of your `project.clj` file:

```Clojure
[datalevin "0.9.0"]
```

If you use [Clojure CLI](https://clojure.org/guides/deps_and_cli) and
`deps.edn`, declare the dependency like so:

```Clojure
{:deps {datalevin/datalevin {:mvn/version "0.9.0"}
        com.cognitect/transit-clj {:mvn/version "1.0.329"}}}
```

This library supports Java 8 and above.

**Important:**  For JVM version newer than 11, you need to add the following JVM options:
```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

Or you will get errors such as "Could not initialize class
org.lmdbjava.ByteBufferProxy", and so on.

For `lein`, add a top level `:jvm-opts` in your `project.clj` like so:

```
:jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
           "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]

```

For `dep.edn`, this is known to work:

```
:aliases {:jvm-base
           {:jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
                       "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]}}
```
Then `clj -A:jvm-base`

### Other JVM Languages

Datalevin can be used in other JVM languages than Clojure, such as Java, Scala, Kotlin,
and so on, by using the official [Clojure Java
API](http://clojure.github.io/clojure/javadoc/clojure/java/api/package-summary.html).
If you have done so, a PR to document your example will be welcome here. In
addition, one can build a Datalevin wrapper in other JVM languages this way, and
we will be happy to link to it here if you have done so.

### GraalVM Native Image

If your application depends on the Datalevin library and you want to compile your
application to a GraalVM native image, put `org.clojars.huahaiy/datalevin-native`
instead  (they have the same version number) in your `project.clj` or `deps.edn` file.

This is necessary because `datelevin-native` artifact contains GraalVM specific
code that should not appear in a regular JVM library. See also this
[note](https://github.com/juji-io/datalevin/tree/master/native#compiling-datalevin-dependency-to-native-image).

## Command Line Tool

A command line tool [`dtlv`](https://github.com/juji-io/datalevin/blob/master/doc/dtlv.md) is built to work with Datalevin databases in shell
scripting, doing work such as database backup/compaction, data import/export,
query/transaction execution, server administration, and so on. The same binary
can also run as a Datalevin server. This tool also includes a REPL with a Clojure
interpreter, in addition to support all the database functions.

Unlike many other database software (e.g. SQLite, Postgres, etc.) that introduces
a separate language for the command line, the same Clojure
code works in both Datalevin library and Datalevin command line tool.

A native Datalevin is built by compiling into [GraalVM native
image](https://www.graalvm.org/reference-manual/native-image/). In addition to
fast startup times, it should also have better index access speed, for the
native image version does not incur JNI overhead and uses a comparator written
in C, see [blog
post](https://yyhh.org/blog/2021/02/writing-c-code-in-javaclojure-graalvm-specific-programming/).

Here is how to get the Datalevin command line tool:

### MacOS and Linux Package

Install using [homebrew](https://brew.sh/)

```console
brew install huahaiy/brew/datalevin
```

### Windows Package

Install using [scoop](https://scoop.sh/)

```console
# Note: if you get an error you might need to change the execution policy (i.e. enable Powershell) with
# Set-ExecutionPolicy RemoteSigned -scope CurrentUser
Invoke-Expression (New-Object System.Net.WebClient).DownloadString('https://get.scoop.sh')

scoop bucket add scoop-clojure https://github.com/littleli/scoop-clojure
scoop bucket add extras
scoop install datalevin
```

### Docker

```console
docker pull huahaiy/datalevin
```
See [README on Docker hub](https://hub.docker.com/r/huahaiy/datalevin) for usage.

### Direct Download

Or download the executable binary from github:

* [Linux](https://github.com/juji-io/datalevin/releases/download/0.9.0/dtlv-0.9.0-ubuntu-latest-amd64.zip)
  on x86-64 (AMD64)
* [MacOS](https://github.com/juji-io/datalevin/releases/download/0.9.0/dtlv-0.9.0-macos-latest-aarch64.zip)
  on arm64 (AARCH64)
* [MacOS](https://github.com/juji-io/datalevin/releases/download/0.9.0/dtlv-0.9.0-macos-latest-amd64.zip)
  on x86-64 (AMD64)
* [Windows](https://github.com/juji-io/datalevin/releases/download/0.9.0/dtlv-0.9.0-windows-amd64.zip) on x86-64 (AMD64)

Unzip, put it on your path, and execute `dtlv help`:

```console
  Datalevin (version: 0.9.0)

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


Starting `dtlv` without any arguments goes into the console:

```console
  Datalevin (version: 0.9.0)

  Type (help) to see available functions. Some Clojure core functions are also available.
  Type (exit) to exit.

user> (help)

In addition to some Clojure core functions, the following functions are available:

In namespace datalevin.core

add                   add-doc               clear                 clear-dbi
close                 close-db              close-kv              closed-kv?
closed?               commit                conn-from-datoms      conn-from-db
conn?                 copy                  create-conn           datom
datom-a               datom-e               datom-v               datom?
datoms                db                    db?                   dir
doc-count             doc-indexed?          doc-refs              drop-dbi
empty-db              entid                 entity                entity-db
entries               get-conn              get-first             get-range
get-some              get-value             hexify-string         index-range
init-db               k                     list-dbis             listen!
new-search-engine     open-dbi              open-kv               opts
pull                  pull-many             put-buffer            q
range-count           range-filter          range-filter-count    read-buffer
remove-doc            reset-conn!           resolve-tempid        retract
rseek-datoms          schema                search                search-index-writer
seek-datoms           stat                  tempid                touch
transact              transact!             transact-async        transact-kv
unhexify-string       unlisten!             update-schema         v
visit                 with-conn             write

In namespace datalevin.interpret

definterfn            exec-code             inter-fn              inter-fn-from-reader
inter-fn?             load-edn

In namespace datalevin.client

assign-role           close-database        create-database       create-role
create-user           disconnect-client     drop-database         drop-role
drop-user             grant-permission      list-databases        list-databases-in-use
list-role-permissions list-roles            list-user-permissions list-user-roles
list-users            new-client            open-database         query-system
reset-password        revoke-permission     show-clients          withdraw-role

Can call function without namespace: (<function name> <arguments>)

Type (doc <function name>) to read documentation of the function

user>

```

You may want to launch `dtlv` in `rlwrap` to get a better REPL experience.

### Uberjar

A JVM
[uberjar](https://github.com/juji-io/datalevin/releases/download/0.9.0/datalevin-0.9.0-standalone.jar)
is downloadable to use as the command line tool. It is useful when one wants to
run a Datalevin server and needs the efficiency of JVM's JIT, as GraalVM native
image is AOT and not as efficient as JVM for long running programs, or when a
pre-built native version is not available for your platform. For example,
assuming your Java is newer than version 11:

```console
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar datalevin-0.9.0-standalone.jar
```
This will start the Datalevin REPL.

```console
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar datalevin-0.9.0-standalone.jar serv -r /tmp/test-server
```
Will run the Datalevin server on default port 8898, with root data path at
`/tmp/test-server`.

## Babashka Pod

The `dtlv` executable can also run as a
[Babashka](https://github.com/babashka/babashka)
[pod](https://github.com/babashka/pods). It is also possible to download
Datalevin directly from [pod
registry](https://github.com/babashka/pod-registry) within a Babashka script
(not all versions are registered):

```
#!/usr/bin/env bb

(require '[babashka.pods :as pods])
(pods/load-pod 'huahaiy/datalevin "0.9.0")

```

For pod usage, an extra macro `defpodfn` is provided to define a custom function
that can be used in a query, e.g.:

```console
$ rlwrap bb
Babashka v1.3.181 REPL.
Use :repl/quit or :repl/exit to quit the REPL.
Clojure rocks, Bash reaches.

user=> (require '[babashka.pods :as pods])
nil
user=> (pods/load-pod "dtlv")
#:pod{:id "pod.huahaiy.datalevin"}
user=>  (require '[pod.huahaiy.datalevin :as d])
nil
user=> (d/defpodfn custom-fn [n] (str "hello " n))
#:pod.huahaiy.datalevin{:inter-fn custom-fn}
user=> (d/q '[:find ?greeting :where [(custom-fn "world") ?greeting]])
#{["hello world"]}
user=> (def conn (d/get-conn "/tmp/bb-test"))
#'user/conn
user=> (d/transact! conn [{:name "hello"}])
{:datoms-transacted 1}
user=> (d/q '[:find ?n :where [_ :name ?n]] (d/db conn))
#{["hello"]}
user=> (d/close conn)
nil
user=>
```
The example above uses `dtlv` binary in the PATH.
