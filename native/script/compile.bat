
if "%GRAALVM_HOME%"=="" (
    echo Please set GRAALVM_HOME
    exit /b
)

set JAVA_HOME=%GRAALVM_HOME%\bin
set PATH=%GRAALVM_HOME%\bin;%PATH%

call %GRAALVM_HOME%\bin\gu.cmd install native-image

call ..\lein.bat with-profile test0-uberjar do clean, uberjar
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

call ..\lein.bat with-profile test1-uberjar do clean, uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-R:MaxHeapSize=5g" ^
  "-jar" "target/test1.uberjar.jar" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv-test1

if %errorlevel% neq 0 exit /b %errorlevel%

.\dtlv-test1 -Xmx5g

if %errorlevel% neq 0 exit /b %errorlevel%

del dtlv-test1

call ..\lein.bat with-profile test2-uberjar do clean, uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-R:MaxHeapSize=5g" ^
  "-jar" "target/test2.uberjar.jar" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv-test2

if %errorlevel% neq 0 exit /b %errorlevel%

.\dtlv-test2 -Xmx5g

if %errorlevel% neq 0 exit /b %errorlevel%

del dtlv-test2

call ..\lein.bat with-profile native-uberjar uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-jar" "target/main.uberjar.jar" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv

if %errorlevel% neq 0 exit /b %errorlevel%
