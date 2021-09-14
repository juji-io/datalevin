
if "%GRAALVM_HOME%"=="" (
    echo Please set GRAALVM_HOME
    exit /b
)

set JAVA_HOME=%GRAALVM_HOME%\bin
set PATH=%GRAALVM_HOME%\bin;%PATH%

call %GRAALVM_HOME%\bin\gu.cmd install native-image

set PWD=%cd%
set CPATH=%PWD%\src\c

cd %CPATH%

mkdir build
cd build

cmake .. ^
    -G "NMake Makefiles" ^
    -DCMAKE_BUILD_TYPE:STRING=RELEASE ^
    -DCMAKE_INSTALL_PREFIX=%CPATH% ^
    -DCLOSE_WARNING=on ^
    -DBUILD_TEST=off ^
    -DBUILD_SHARED_LIBS=off
nmake install

cd %PWD%

call ..\lein.bat with-profile test-uberjar do clean, uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-jar" "target/test.uberjar.jar" ^
  "-H:CLibraryPath=%DTVL_NATIVE_EXTRACT_DIR%" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv-test

if %errorlevel% neq 0 exit /b %errorlevel%

.\dtlv-test

if %errorlevel% neq 0 exit /b %errorlevel%

call ..\lein.bat uberjar
if %errorlevel% neq 0 exit /b %errorlevel%

call %GRAALVM_HOME%\bin\native-image.cmd ^
  "-jar" "target/main.uberjar.jar" ^
  "-H:CLibraryPath=%DTVL_NATIVE_EXTRACT_DIR%" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv

if %errorlevel% neq 0 exit /b %errorlevel%
