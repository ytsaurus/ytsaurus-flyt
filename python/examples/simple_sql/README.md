# simple_sql

Run a Flink SQL script on YTsaurus without writing Python:

```bash
flyt run examples/simple_sql/wordcount.sql
flyt run --mode application --taskmanagers 2 examples/simple_sql/wordcount.sql
```

See "Flink SQL scripts" in the top-level README for statement handling.
