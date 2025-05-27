# Installation

Datalevin can be installed with different methods, depending on how you plan to use it.

## Clojure Library

The core of Datalevin is a JVM Clojure library, simply add it to your Clojure
project as a dependency and start using it!

If you use [Leiningen](https://leiningen.org/) build tool, add this to the
`:dependencies` section of your `project.clj` file:

```Clojure
[datalevin "0.9.22"]
```

If you use [Clojure CLI](https://clojure.org/guides/deps_and_cli) and
`deps.edn`, declare the dependency like so:

```Clojure
{:deps {datalevin/datalevin {:mvn/version "0.9.22"}}}
```

This library supports Java 17 and above.

### Native Dependencies

For now, Datalevin requires some system libraries to be present, such as `libc`,
`libomp`, and `libmvec`. These are normally easy to get:

* Linux needs [OpenMP](https://www.openmp.org/) and [Vectorized
  Math](https://sourceware.org/glibc/wiki/libmvec) from GCC, e.g. on
  Debian/Ubuntu, `apt-get install libgomp1` or `apt-get install g++-12 gcc-12`.

* MacOSX needs the same libraries as the above from Clang, e.g. `brew
  install libomp llvm`

Otherwise, Datalevin may fail to load and report
`java.lang.UnsatisfiedLinkError`.

### Unreleased Code

The `master` branch of this project is kept fully functional, so if you
need to use some yet-to-be released fixes or features, you can declare the
dependency in `deps.edn` (remember to change the `:sha`):

```Clojure
{:deps {datalevin/datalevin
        {:git/url "https://github.com/juji-io/datalevin.git"
         :sha "d839883e4dec35b89442fa8ebbd50c99a2b25a50"}}}
```
Make sure to go to `~/.gitlibs/libs/datalevin/datalevin/$SHA` and run `lein test` to
compile and run tests first.

### Performance Tip

To obtain better performance (about 5% to 20%), you may
want to add the following JVM options to your project:
```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

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

### Java 24

You want to add `--enable-native-access=ALL-UNNAMED` JVM options to disable
warnings about native access on Java 24.

### Other JVM Languages

Datalevin can be used in other JVM languages than Clojure, such as Java, Scala, Kotlin,
and so on, by using the official [Clojure Java
API](http://clojure.github.io/clojure/javadoc/clojure/java/api/package-summary.html).
If you have done so, a PR to document your example will be welcome here. In
addition, one can build a Datalevin wrapper in other JVM languages this way, and
we will be happy to link to it here if you have done so.

## Command Line Tool

A command line tool
[`dtlv`](https://github.com/juji-io/datalevin/blob/master/doc/dtlv.md) is built
to work with Datalevin databases in shell scripting, doing work such as database
backup/compaction, data import/export, query/transaction execution, server
administration, and so on. The same binary can also run as a Datalevin server.
This tool also includes a REPL with a Clojure interpreter, in addition to
support all the database functions.

Unlike many other database software (e.g. SQLite, Postgres, etc.) that introduces
a separate language for the command line, the same Clojure
code works in both Datalevin library and Datalevin command line tool.

A native Datalevin is built by compiling into [GraalVM native
image](https://www.graalvm.org/reference-manual/native-image/).

These are the ways to get the Datalevin command line tool:

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

* [Linux](https://github.com/juji-io/datalevin/releases/download/0.9.22/dtlv-0.9.22-ubuntu-22.04-amd64.zip)
  on x86_64 (AMD64)
* [Linux](https://github.com/juji-io/datalevin/releases/download/0.9.22/dtlv-0.9.22-ubuntu-latest-aarch64.zip)
  on arm64 (AARCH64)
* [MacOS](https://github.com/juji-io/datalevin/releases/download/0.9.22/dtlv-0.9.22-macos-latest-aarch64.zip)
  on arm64 (AARCH64)
* [MacOS](https://github.com/juji-io/datalevin/releases/download/0.9.22/dtlv-0.9.22-macos-13-amd64.zip)
  on x86_64 (AMD64)
* [Windows](https://github.com/juji-io/datalevin/releases/download/0.9.22/dtlv-0.9.22-windows-amd64.zip) on x86-64 (AMD64)

Unzip to get a `dtlv` executable, put it on your path.

You may want to launch `dtlv` in `rlwrap` to get a better REPL experience.

### Uberjar

A JVM
[uberjar](https://github.com/juji-io/datalevin/releases/download/0.9.22/datalevin-0.9.22-standalone.jar)
is downloadable to use as the command line tool. It is useful when one wants to
run a Datalevin server and needs the efficiency of JVM's JIT, as GraalVM native
image is not as efficient as Hotspot JVM for long running programs, or when a
pre-built native version is not available for your platform. For example:

```console
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar datalevin-0.9.22-standalone.jar
```
This will start the Datalevin REPL.

```console
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar datalevin-0.9.22-standalone.jar serv -r /tmp/test-server
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
(pods/load-pod 'huahaiy/datalevin "0.9.22")

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
user=> (require '[pod.huahaiy.datalevin :as d])
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
