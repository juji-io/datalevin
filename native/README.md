# Native Datalevin

Datalevin can be compiled as a GraalVM native image.

## Compilation

1. [Install GraalVM](https://www.graalvm.org/docs/getting-started/#install-graalvm)
2. [Intsall GraalVM native image](https://www.graalvm.org/reference-manual/native-image/)
3. Install liblmdb-dev, depending on your OS. E.g. `apt install liblmdb-dev` on Debian/Ubuntu, `brew install lmdb` on MacOS.
4. Run `script/compile`. 

If the compilation is successful, two binaries will appear in this directory: dtlv and dtlv-test. The former is the Datalevin command line shell (work in progress), and the latter runs all the Datalevin tests in native mode.
