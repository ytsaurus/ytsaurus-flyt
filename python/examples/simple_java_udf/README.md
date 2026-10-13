# Simple Java UDF

PyFlink job with a Java `ScalarFunction` (`org.example.SimpleHashUdf`). The UDF JAR is shipped in `flink/lib` inside JM/TM by flyt.

1. Build the JAR: `cd udf && ./gradlew jar`. Output: `build/libs/simple-java-udf-1.0.0.jar`.
2. Upload to Cypress (from `udf/`, after step 1). Example: `yt write-file //sys/flink/libraries/simple-java-udf-1.0.0.jar < build/libs/simple-java-udf-1.0.0.jar`. Use a destination under your profile `jar_scan_folder`.

From the repo `python/` directory:

```bash
export YT_TOKEN=...
# 1. Build + upload the runtime layer once (the profile already points squashfs_layer_paths here).
flyt build layer --upload //sys/flink/flyt-flink120-py310.squashfs
# 2. Import and select the profile.
flyt profile import examples/simple_java_udf/profile.yaml --as simple_java_udf # [--proxy URL] [--pool POOL]
flyt profile select simple_java_udf
# 3. Launch.
flyt run "examples/simple_java_udf/pipeline.py"
```
