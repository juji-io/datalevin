# Benchmark

This contains the original benchmarks from [Datascript](https://github.com/tonsky/datascript), with code for Datalevin added.

To run the benchmarks, you need to have the [Clojure CLI](https://clojure.org/guides/deps_and_cli) installed on your system already. Then run these commands in the project root directory.

```
lein javac
cd bench
./bench.clj
```

If you are on a version of JVM newer than JDK 8, you may get some warnings. Please ignore them. 

Some of the benchmarks are commented out for they take a long time to run. You can uncomment the appropriate lines in [default-benchmarks](https://github.com/juji-io/datalevin/blob/master/bench/bench.clj#L112) in `bench.clj` to run them.

You can also comment out the appropriate line to run the same benchmark for DataScript.  For more comparisons with other alternatives, you may also consult [this fork](https://github.com/joinr/datalevinbench).
