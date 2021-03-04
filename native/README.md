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
application into GraalVM native image, you need to integrate the following steps in your
native image build:

1. Merge [Datalevin's reflect-config.json](https://github.com/juji-io/datalevin/releases/download/0.4.16/reflect-config.json) into yours.
2. [Download our C source and build]() and add to your [CLibraryPath]()

Native Datalevin contains GraalVM specific code, therefore it is not enough for us to bundle the built
native library, because you need our header file to build a native image. Since you need to download our header file anyway,
you might as well download the source tree and build it yourself. It is less error prone.
