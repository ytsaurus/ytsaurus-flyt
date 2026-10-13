# Simple wordcount

Minimal PyFlink job: `datagen`, count, `print`. No extra JARs.

From the repo `python/` directory:

```bash
export YT_TOKEN=...
# 1. Build + upload the runtime layer once (the profile already points squashfs_layer_paths here).
flyt build layer --upload //sys/flink/flyt-flink120-py310.squashfs
# 2. Import and select the profile.
flyt profile import examples/simple_wordcount/profile.yaml --as simple_wordcount # [--proxy URL] [--pool POOL]
flyt profile select simple_wordcount
# 3. Launch.
flyt run "examples/simple_wordcount/pipeline.py"
```

Kind: [../kind/README.md](../kind/README.md).
