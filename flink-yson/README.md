# Apache Flink YSON Format

This project provides Apache Flink format support for [YSON (Yet anOther Serialization Notation)](https://ytsaurus.tech/docs/en/user-guide/storage/yson), the native serialization format used by YTsaurus.

## Supported Flink Versions

The format officially supports Apache Flink version `1.20.X`, but compatibility with other versions is not excluded.

## Installation

Maven

```xml
<dependency>
    <groupId>tech.ytsaurus.flyt.formats.yson</groupId>
    <artifactId>flink-yson</artifactId>
    <version>1.1.0</version>
    <classifier>all</classifier>
</dependency>
```

Gradle

```kotlin
implementation("tech.ytsaurus.flyt.formats.yson:flink-yson:1.1.0:all")
```

## Building from Source

### Prerequisites

- Git
- Gradle 8.x (we recommend 8.14.3)
- Java 11

### Build Steps

```bash
git clone https://github.com/ytsaurus/ytsaurus-flyt.git
cd ytsaurus-flyt
gradle shadowJar -p flink-yson

# To see the assembled artifact
ls flink-yson/build/libs
```

## Contributing

Contributions are welcome! Please see the [CONTRIBUTING.md](../CONTRIBUTING.md) file for guidelines.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](../LICENSE) file for details.