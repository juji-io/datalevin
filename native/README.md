# Native Datalevin

Datalevin can be compiled as a GraalVM native image.

## Build Native Datalevin

1. [Install GraalVM](https://www.graalvm.org/docs/getting-started/#install-graalvm)
2. [Intsall GraalVM native image](https://www.graalvm.org/reference-manual/native-image/)
3. Install liblmdb-dev, depending on your OS. E.g. `apt install liblmdb-dev` on Debian/Ubuntu, `brew install lmdb` on MacOS.
4. Run `script/compile`.

If the compilation is successful, two binaries will appear in this directory:
`dtlv ` and `dtlv-test`. The former is the Datalevin command line shell, and the latter runs all the Datalevin tests in native mode.

## Compiling Datalevin Dependency to Native Image

If your Clojure application depends on Datalevin, and you want to compile your
application into GraalVM native image, you need to integrate the following steps in your
native image build:

1. Merge [Datalevin's reflect-config.json](https://github.com/juji-io/datalevin/releases/download/0.4.14/reflect-config.json) into yours.
2. Install liblmdb-dev, see step 3 above.
3. [Build libdtlv.a](https://github.com/juji-io/datalevin/blob/61f9e61b9a12a06beafdedeb810dd9aa9e43d722/native/script/compile#L19) and put it on your [CLibraryPath](https://github.com/juji-io/datalevin/blob/61f9e61b9a12a06beafdedeb810dd9aa9e43d722/native/script/compile#L35)

Step 1 seems to be unavoidable. Step 2 and 3 will be removed in the future when
[#38](https://github.com/juji-io/datalevin/issues/38) is resolved.
