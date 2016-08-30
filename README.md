sortbenchmark.org Cloudsort with Apache Flink.

The file sortbenchmark/crc32 contains checksums for the first one million 1,000,000,0000 byte
files (one petabyte), each containing 10,000,000 records. These checksums can be added using
using the following command where COUNT is the number of gigabytes of input data.

```bash
  head -n ${COUNT} checksums | python -c "import sys; print hex(sum(int(l, 16) for l in sys.stdin))[2:].rstrip('L')"
```
