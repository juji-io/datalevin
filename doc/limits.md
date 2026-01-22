# Limitations

* Single writer. See also [transaction](transact.md).

* Attributes must be keywords.

* Attribute names have a length limitation: an attribute name cannot be more
  than 511 bytes long, due to LMDB key size limit.

* Because keys are compared bitwise, for range queries to work as expected on an
  attribute, its `:db/valueType` should be specified.

* `nil` cannot be stored. Floating point `NaN` cannot be stored.

* Big integers do not go beyond the range of `[-2^1015, 2^1015-1]`, the
  unscaled value of big decimal has the same limit.

* The maximum individual value size is 2GB. Limited by the maximum size of
  off-heap byte buffer that can be allocated in JVM.

* The total data size of a Datalevin database has the same limit as LMDB's, e.g.
  128TB on a modern 64-bit machine that implements 48-bit address spaces.

* Currently supports Clojure on JVM 21 or above, but adding support for other
  Clojure-hosting runtimes is possible, since bindings for LMDB
  exist in almost all major languages and are available on most platforms.

* Expect certain native dependencies on the system. mainly libc. Others are bundled
  in the release jar.
