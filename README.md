<img width="64" src="https://raw.githubusercontent.com/ytsaurus/ytsaurus/main/yt/docs/images/logo.png"/><br/>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ytsaurus/ytsaurus-flyt/blob/main/LICENSE)
[![Telegram](https://img.shields.io/badge/chat-on%20Telegram-2ba2d9.svg)](https://t.me/ytsaurus)

## YTsaurus

[Website](https://ytsaurus.tech) |
[Documentation](https://ytsaurus.tech/docs) |
[YouTube](https://www.youtube.com/@ytsaurus)

YTsaurus is a distributed storage and processing platform for big data with support for MapReduce model, a distributed file system and a NoSQL key-value database.

You can read [post about YTsaurus](https://medium.com/p/42e7f5fa5fc6) or check video:

[![video about YTsaurus](https://raw.githubusercontent.com/ytsaurus/ytsaurus/main/yt/docs/images/ytsaurus-promo-video.png)](https://youtu.be/4Q2EB_uimLs)

## FLYT

This repo ships the **Python launcher / CLI** [`ytsaurus-flyt`](python/) for running [PyFlink](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/overview/) jobs inside YTSaurus.

### Quick start

```bash
pip install ytsaurus-flyt
export YT_TOKEN=...
flyt profile add my-cluster --proxy http://localhost:50005
flyt install
flyt run python/examples/simple_wordcount/pipeline.py
```

See [python/README.md](python/README.md) and [python/ARCHITECTURE.md](python/ARCHITECTURE.md).
