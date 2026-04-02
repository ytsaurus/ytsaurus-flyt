# Simple wordcount

Minimal PyFlink job: `datagen`, count, `print`. No extra JARs.

From the repo `python/` directory:

```bash
export YT_TOKEN=...
flyt profile import examples/simple_wordcount/profile.yaml --as simple_wordcount # [--proxy URL] [--pool POOL]
flyt profile select simple_wordcount
flyt install
flyt run "examples/simple_wordcount/pipeline.py"
```

Kind: [../kind/README.md](../kind/README.md).
