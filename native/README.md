# Native Datalevin

Datalevin can be compiled as a GraalVM native image.

## Build Native Datalevin

Assuming that you can build JVM Datalevin already, i.e. you have
[JDK](https://openjdk.java.net/) and [lein](https://leiningen.org/), here are
the steps to build native Datalevin binary on your platform.

### MacOS/Linux

1. [Install GraalVM](https://www.graalvm.org/docs/getting-started/#install-graalvm)
2. [Intsall GraalVM native image](https://www.graalvm.org/reference-manual/native-image/)
3. Run `script/compile`.

If the compilation is successful, two binaries will appear in this directory:
`dtlv ` and `dtlv-test0`. The former is the Datalevin command line shell, and the
latter runs core Datalevin tests in native mode. These are standalone
executable that you can immediately run.

Because of the recent libc version problems on the latest Ubuntu, the Linux
build should preferably be static using musl libc, so it is preferable to
run `script/setup-musl`, then`script/compile-static` on the x86 Linux platform
for step 3.

### Windows

Same as the above, except to run `script/compile.bat` instead in step 3.

If the build is successful, you will get `dtlv.exe` and `dtlv-test0.exe`.


## Compiling Datalevin Dependency to Native Image

If your application depends on Datalevin library, and you want to compile your
application into a GraalVM native image, you need to use
**`org.clojars.huahaiy/datalevin-native`** as the dependency, instead of
`datalevin/datalevin`, because `org.clojars.huahaiy/datalevin-native` includes
some [GraalVM specific native
code](https://yyhh.org/blog/2021/02/writing-c-code-in-javaclojure-graalvm-specific-programming/)
that will fail to compile in regular JVM. It also includes pre-compiled native
dependencies for these platforms:

* Linux x86-64
* MacOS arm64 (i.e. Apple Silicon M series)
* MacOS x86-64
* Windows x86-64

First build an uberjar of your application, then compile it with `native-image`
command.

Like all Clojure applications, class initialization needs to be done at [native image
build time](https://github.com/clj-easy/graal-docs#class-initialization), i.e.
add `--features=InitAtBuildTimeFeature` to `native-image` command.

During the native image build time, our class initialization code extracts
native libraries from the `org.clojars.huahaiy/datalevin-native` jar and put
them in the GraalVM's default `CLibraryPath` for the platform (e.g.
`${GRAALVM_HOME}/lib/svm/clibraries/linux-amd64/`). The files will be deleted
upon build completion.

If you are uncomfortable with writing to the default location or lack the write
permission for that directory, you can set an environment variable
`DTLV_LIB_EXTRACT_DIR` in the shell before the native image build, and the
native libraries will then be put there instead. If so, you must also add
`-H:CLibraryPath=${DTLV_LIB_EXTRACT_DIR}` option to `native-image` command. The
directory referred to by the environment variable must exist and is writable.

You also need to have these environment variables set before native image
compilation:

```
export DTLV_COMPILE_NATIVE=true
export USE_NATIVE_IMAGE_JAVA_PLATFORM_MODULE_SYSTEM=false
```

Finally, look at our `script/compile`, or `script/compile-static` file as an example.

For CI/CD on various platforms, you may want to consult our [Github
Action](https://github.com/juji-io/datalevin/blob/master/.github/workflows/release.binaries.yml)
(for Linux/MacOS AMD64),
[Appveoyor](https://github.com/juji-io/datalevin/blob/master/appveyor.yml) (for
Windows), and [Cirrus
CI](https://github.com/juji-io/datalevin/blob/master/.cirrus.yml)(for Apple
Silicon) yaml files for examples.

Native Datalevin libraries are static, so your final application is standalone
and requires only `libc` dependency on the OS to run, unless you compile static
native image, then it is truly standalone, but GraalVM native image only
supports the full static option on x86_64 Linux at the moment.
