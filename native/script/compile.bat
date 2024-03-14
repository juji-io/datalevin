
if "%GRAALVM_HOME%"=="" (
    echo Please set GRAALVM_HOME
    exit /b
)

set JAVA_HOME=%GRAALVM_HOME%\bin
set PATH=%GRAALVM_HOME%\bin;%PATH%
set USE_NATIVE_IMAGE_JAVA_PLATFORM_MODULE_SYSTEM=false

cd ..

REM call lein.bat run

call %GRAALVM_HOME%\bin\gu.cmd install --file native.jar

REM cd native

REM call ..\lein.bat with-profile test0-uberjar do clean, uberjar
REM if %errorlevel% neq 0 exit /b %errorlevel%

REM call %GRAALVM_HOME%\bin\native-image.cmd ^
REM   "-R:MaxHeapSize=5g" ^
REM   "-jar" "target/test0.uberjar.jar" ^
REM   "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
REM   dtlv-test0

REM if %errorlevel% neq 0 exit /b %errorlevel%

REM cd ..

REM native\dtlv-test0 -Xmx5g

REM if %errorlevel% neq 0 exit /b %errorlevel%

REM cd native

REM del dtlv-test0

call ..\lein.bat with-profile native-uberjar uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-jar" "target/main.uberjar.jar" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv

if %errorlevel% neq 0 exit /b %errorlevel%
