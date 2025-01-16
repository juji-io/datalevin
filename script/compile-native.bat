
if "%GRAALVM_HOME%"=="" (
    echo Please set GRAALVM_HOME
    exit /b
)

set JAVA_HOME=%GRAALVM_HOME%\bin
set PATH=%GRAALVM_HOME%\bin;%PATH%
set JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8

call lein.bat run

call lein.bat with-profile test0-uberjar do clean, uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
   "-R:MaxHeapSize=5g" ^
   "-jar" "target/test0.uberjar.jar" ^
   "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
   dtlv-test0

if %errorlevel% neq 0 exit /b %errorlevel%

.\dtlv-test0 -Xmx5g

if %errorlevel% neq 0 exit /b %errorlevel%

del dtlv-test0

call lein.bat with-profile native-uberjar uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-jar" "target/main.uberjar.jar" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv

if %errorlevel% neq 0 exit /b %errorlevel%
