# Native Datalevin

Datalevin can be compiled as a GraalVM native image.

## Build Native Datalevin

1. [Install GraalVM](https://www.graalvm.org/docs/getting-started/#install-graalvm)
2. [Intsall GraalVM native image](https://www.graalvm.org/reference-manual/native-image/)
3. Run `script/compile`.

If the compilation is successful, two binaries will appear in this directory:
`dtlv ` and `dtlv-test`. The former is the Datalevin command line shell, and the latter runs all the Datalevin tests in native mode.

## Compiling Datalevin Dependency to Native Image

If your Clojure application depends on Datalevin, and you want to compile your
application into a GraalVM native image, you need to integrate the following steps in your
native image build script:

1. Merge [Datalevin's reflect-config.json](https://github.com/juji-io/datalevin/releases/download/0.4.16/reflect-config.json) into yours.
2. Download [our C source tree](https://github.com/juji-io/datalevin/tree/master/native/src/c), [run `make`](https://github.com/juji-io/datalevin/blob/25acc097b07ca48626b628849a2c937d755b980c/native/script/compile#L19) in it, and add the path to your [CLibraryPath](https://github.com/juji-io/datalevin/blob/25acc097b07ca48626b628849a2c937d755b980c/native/script/compile#L34)

Step 2 is necessary because native Datalevin contains GraalVM specific code. It is not enough to bundle the built native library, because building native image requires header files. It is less error prone to download the C source tree and replicate how Datalevin compiles native images.
