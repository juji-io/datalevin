@echo off

if "%GRAALVM_HOME%"=="" (
    echo Please set GRAALVM_HOME
    exit /b
)

set JAVA_HOME=%GRAALVM_HOME%\bin
set PATH=%GRAALVM_HOME%\bin;%PATH%

call %GRAALVM_HOME%\bin\gu.cmd install native-image

set PWD=%cd%
set MAIN_JAR=%PWD%\target\main.uberjar.jar
set CPATH=%PWD%\src\c

call lein do clean, uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

cd %CPATH%
make

cd %PWD%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-jar %MAIN_JAR%" ^
  "-H:Name=dtlv" ^
  "-H:+ReportExceptionStackTraces" ^
  "-H:ConfigurationFileDirectories=config" ^
  "-J-Dclojure.spec.skip-macros=true" ^
  "-J-Dclojure.compiler.direct-linking=true" ^
  "-H:CLibraryPath=%CPATH%" ^
  "--initialize-at-build-time"  ^
  "-H:Log=registerResource:" ^
  "--report-unsupported-elements-at-runtime" ^
  "--allow-incomplete-classpath" ^
  "--no-fallback" ^
  "--native-image-info" ^
  "--verbose" ^
  "-J-Xmx6g"

if %errorlevel% neq 0 exit /b %errorlevel%

echo Creating zip archive
jar -cMf dtlv-windows-amd64.zip dtlv.exe
