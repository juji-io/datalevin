# Native Datalevin

Datalevin can be compiled as a GraalVM native image.

## Compiling Datalevin Dependency to Native Image

If your Clojure application depends on Datalevin, and you want to compile your
application into a GraalVM native image, you need to integrate the following
steps in your native image build script:

1. Merge [Datalevin's
   reflect-config.json](https://github.com/juji-io/datalevin/releases/download/0.4.25/reflect-config.json)
   into yours.
2. Download [Datalevin's C
   source](https://github.com/juji-io/datalevin/releases/download/0.4.25/datalevin-c-source-0.4.25.zip),
   unzip, run [`make`](https://github.com/juji-io/datalevin/blob/25acc097b07ca48626b628849a2c937d755b980c/native/script/compile#L19) (or [`cmake`](https://github.com/juji-io/datalevin/blob/869f4099cf12eb4a21a7518630088d8e9f3bb324/native/script/compile.bat#L20) on Windows) in it, and add the path to your
   [CLibraryPath](https://github.com/juji-io/datalevin/blob/25acc097b07ca48626b628849a2c937d755b980c/native/script/compile#L34).


Step 2 is necessary because native Datalevin contains [GraalVM specific
code](https://yyhh.org/blog/2021/02/writing-c-code-in-javaclojure-graalvm-specific-programming/),
and compiling them requires our C header files. It is easier and less error
prone to download our C source tree and replicate how Datalevin compiles native
image.

For CI/CD, you may want to consult our simple [Github
Action](https://github.com/juji-io/datalevin/blob/master/.github/workflows/release.binaries.yml)
(for Linux/MacOS) and
[Appveoyor](https://github.com/juji-io/datalevin/blob/master/appveyor.yml) (for
Windows) yaml config files. Below are some descriptions of what they do:

## Build Native Datalevin

Assuming that you can build JVM Datalevin already, i.e. you have
[JDK](https://openjdk.java.net/) and [lein](https://leiningen.org/), here are
the steps to build native Datalevin on your platform.

### Linux/MacOS

1. [Install GraalVM](https://www.graalvm.org/docs/getting-started/#install-graalvm)
2. [Intsall GraalVM native image](https://www.graalvm.org/reference-manual/native-image/)
3. Run `script/compile`.

This requires essential Unix build tools, i.e. `gcc`, `make`, and so on. I.e.
you need [Xcode Command Line Tools](https://developer.apple.com/xcode/) on
MacOS; `sudo apt-get install build-essential` on Debian/Ubuntu, for example.

If the compilation is successful, two binaries will appear in this directory:
`dtlv ` and `dtlv-test`. The former is the Datalevin command line shell, and the
latter runs all the Datalevin tests in native mode.

### Windows

Same as the above, except to run `script/compile.bat` instead in step 3.

This requires [Visual
Studio](https://visualstudio.microsoft.com/vs/older-downloads/) (Visual
Studio 2019 and 2017 both work) and [cmake](https://cmake.org/).

If the build is successful, you will get `dtlv.exe` and `dtlv-test.exe`.
