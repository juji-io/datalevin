
if "%GRAALVM_HOME%"=="" (
    echo Please set GRAALVM_HOME
    exit /b
)

set JAVA_HOME=%GRAALVM_HOME%\bin
set PATH=%GRAALVM_HOME%\bin;%PATH%
set USE_NATIVE_IMAGE_JAVA_PLATFORM_MODULE_SYSTEM=false

call %GRAALVM_HOME%\bin\gu.cmd install native-image

call ..\lein.bat with-profile test0-uberjar do clean, uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
   "-R:MaxHeapSize=5g" ^
   "-jar" "target/test0.uberjar.jar" ^
   "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  "--features=InitAtBuildTimeFeature" ^
   dtlv-test0

if %errorlevel% neq 0 exit /b %errorlevel%

cd ..

native\dtlv-test0 -Xmx5g

if %errorlevel% neq 0 exit /b %errorlevel%

cd native

del dtlv-test0

call ..\lein.bat with-profile native-uberjar uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-jar" "target/main.uberjar.jar" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  "--features=InitAtBuildTimeFeature" ^
  dtlv

if %errorlevel% neq 0 exit /b %errorlevel%
