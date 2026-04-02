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

Integration between Apache Flink and YTsaurus. Java connectors and formatters live in the main [ytsaurus](https://github.com/ytsaurus/ytsaurus) repository; this repo ships the **Python launcher** [`ytsaurus-flyt`](python/).

### Quick start

```bash
pip install ytsaurus-flyt
export YT_TOKEN=...
flyt profile add my-dev --proxy http://localhost:50005 --pool default
flyt install
flyt run path/to/pipeline.py
```

See [python/README.md](python/README.md) and [python/ARCHITECTURE.md](python/ARCHITECTURE.md).
