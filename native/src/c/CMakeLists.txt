CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(dtlv VERSION  0.9.24 LANGUAGES  C)

# Compilation switch to turn off compilation warning
OPTION(CLOSE_WARNING "close warning,default off" OFF)
# Compile switch, used to specify whether to compile test code
OPTION(BUILD_TEST "build test program,default off" OFF)

# for #include <unistd.h>
INCLUDE_DIRECTORIES( ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/lmdb/libraries/liblmdb )

# option
IF( CLOSE_WARNING)
    #close warning
    ADD_COMPILE_OPTIONS(/wd4267/wd4996/wd4477/wd4244/wd4146/wd4333/wd4172)
    ADD_DEFINITIONS(-D_CRT_SECURE_NO_WARNINGS)
ENDIF(CLOSE_WARNING)

# lmdb library (static)
ADD_LIBRARY(lmdb STATIC lmdb/libraries/liblmdb/mdb.c lmdb/libraries/liblmdb/midl.c)

# dtlv library (static)
ADD_LIBRARY(dtlv STATIC dtlv.c)

INSTALL (TARGETS lmdb dtlv
    ARCHIVE DESTINATION .)
