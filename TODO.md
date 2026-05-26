# TODO

## E2E Tests: Java/Python-written PK Table Fixtures

### Context
Go unit tests cover the merge pipeline components (IntervalPartition, SortMergeReader,
DeduplicateMergeFunction) but there are no integration tests reading real Paimon tables
written by Java or Python. This is the most important gap vs. paimon-python's test suite.

### Decision needed
How to generate fixtures — three options:

1. **Python-written fixtures (recommended)** — use paimon-python's write API to generate
   fixture tables, commit them to paimon-go/testdata/fixtures/. No JVM needed. Tests Go
   reading Python-written data. Run gen script once; output is version-controlled.

2. **Java-written, committed to repo** — run JavaPyE2ETest once via Maven, commit the
   resulting warehouse directory. True cross-language. Requires Maven once but CI just
   reads committed files.

3. **Java-written, generated in CI** — CI runs Maven before Go e2e tests. Most authentic,
   adds JVM dependency to every CI run.

### Fixture schema (recommended: simple first)
Simple schema to focus on merge pipeline correctness:
- id INT (PK), name STRING, value DOUBLE, dt STRING (partition key)
- bucket = 2, file.format = parquet
- 2 commits: rows 1–4 in commit 1; row 2 updated + row 5 added in commit 2
- Expected after merge: 5 rows, id=2 has updated value

Full schema (mixed_test_pk_tablej_parquet from JavaPyE2ETest.java) available later:
- id INT (PK), name STRING, category STRING (partition), value DOUBLE,
  ts TIMESTAMP(6), ts_ltz TIMESTAMP_LTZ, t TIME, bin_data BINARY(20),
  metadata ROW<source STRING, created_at BIGINT, location ROW<city STRING, country STRING>>
- 7 rows, 1 commit, bucket=2
- Note: ROW/BINARY/TIME Arrow mapping needs verification before using this schema

### Fixture generation script
paimon-go/testdata/gen_fixtures.py — uses paimon-python write API, outputs to
paimon-go/testdata/fixtures/. Run once; commit output.

### Test file: paimon-go/read/e2e_test.go
Build tag: //go:build e2e  →  run with: go test -tags e2e ./read/...

Tests to implement:
1. TestE2E_PK_Deduplicate    — reads pk_basic fixture, asserts 5 rows, id=2 has updated value
2. TestE2E_PK_DeleteDropped  — fixture with explicit DELETE, asserts row is absent
3. TestE2E_PK_Partitioned    — reads partitioned table, asserts correct row count per partition
4. TestE2E_PK_NullPartition  — null partition value routes to __DEFAULT_PARTITION__, readable
5. TestE2E_PK_Filter         — partition predicate pushdown, asserts only matching rows
6. TestE2E_PK_Projection     — column projection, asserts output schema has only requested cols

### Reference: Python e2e test infrastructure
- Java fixture writer: paimon-core/src/test/java/org/apache/paimon/JavaPyE2ETest.java
- Python e2e tests:    paimon-python/pypaimon/tests/e2e/java_py_read_write_test.py
- Warehouse path (Java side): ../paimon-python/pypaimon/tests/e2e/warehouse/
- No pre-built fixtures exist; generated at test runtime by Java then read by Python

### Remaining coverage gaps vs. paimon-python (beyond e2e)
- Deletion vector (DV) support — paimon-python has 4 DV tests; Go has none; DV files
  referenced via ExtraFiles in DataFileMeta, not yet decoded
- Schema evolution / data evolution — new columns added post-creation; Go reads missing
  columns as nulls (partial support) but no dedicated tests
- Multi-format tests — Python tests ORC, Avro, Lance; Go is Parquet-only by design
  (unsupported format error path also untested)
- Incremental / timestamp range reads — Python has 2 tests; Go streaming covers append
  only, not timestamp-bounded batch reads
- Concurrent write safety — not applicable (Go is read-only)
- Partition filter pushdown on PK tables — no dedicated test in Go
