lib_lmdb  := libs/lmdb/libraries/liblmdb
lib_dtlv  := native/src/c
libraries := $(lib_lmdb) $(lib_dtlv)

.PHONY: all $(libraries)
all: $(libraries)

$(libraries): 
		$(MAKE) --directory=$@ $(TARGET)

$(lib_dtlv): $(lib_lmdb)

