READ_PATTERNS: tuple[str, ...] = (
    "spark.read",
    ".read.format(",
    ".read.option(",
    ".read.csv(",
    ".read.parquet(",
    "read_sql(",
    "requests.get(",
    "requests.post(",
    "requests.put(",
    "requests.delete(",
    "read_ndw(",
)

WRITE_PATTERNS: tuple[str, ...] = (
    ".write",
    ".save(",
    ".saveAsTable(",
    ".insertInto(",
    ".to_sql(",
    "write_ndw(",
    "createOrReplaceTempView(",
)

