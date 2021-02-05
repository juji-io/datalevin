#include <lmdb.h>

int dtlv_cmp_memn(const MDB_val *a, const MDB_val *b);

int dtlv_set_comparator(MDB_txn *txn, int dbi);
