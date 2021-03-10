# cmake project script for lmdb 0.9.24
CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(lmdb VERSION  0.9.24 LANGUAGES  C)

# Compilation switch to turn off compilation warning
OPTION(CLOSE_WARNING "close warning,default off" OFF)
# Compile switch, used to specify whether to compile test code
OPTION(BUILD_TEST "build test program,default off" OFF)

IF (MSVC)
    # for #include <unistd.h>
    INCLUDE_DIRECTORIES( ${CMAKE_INSTALL_PREFIX} )
ENDIF(MSVC)
# option
IF( CLOSE_WARNING)
    #close warning
    IF (MSVC)
        ADD_COMPILE_OPTIONS(/wd4267/wd4996/wd4477/wd4244/wd4146/wd4333/wd4172)
        ADD_DEFINITIONS(-D_CRT_SECURE_NO_WARNINGS)
    ELSE()
        ADD_COMPILE_OPTIONS(-Wno-return-local-addr)
    ENDIF(MSVC)
ENDIF(CLOSE_WARNING)

# lmdb library (static)
ADD_LIBRARY(lmdb STATIC mdb.c midl.c)
IF (UNIX)
    SET(CMAKE_EXE_LINKER_FLAGS "-pthread ${CMAKE_EXE_LINKER_FLAGS}")
    SET(CMAKE_SHARED_LINKER_FLAGS "-pthread ${CMAKE_SHARED_LINKER_FLAGS}")
ENDIF (UNIX)

INSTALL (TARGETS lmdb
    LIBRARY DESTINATION .
    ARCHIVE DESTINATION .)
