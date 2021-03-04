lib_lmdb  := libs/lmdb/libraries/liblmdb
lib_dtlv  := native/src/c
libraries := $(lib_lmdb) $(lib_dtlv)

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
		$(MAKE) --directory=$@ liblmdb.a

$(lib_dtlv): $(lib_lmdb)
		$(MAKE) --directory=$@
