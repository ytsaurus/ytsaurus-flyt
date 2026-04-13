# Examples

See [../README.md](../README.md) for install and `flyt` commands.

Example profiles default to `squashfs_layer_delivery: sandbox_unpack` so they work locally and on Kind without extra setup. For production clusters, switch to `layer_paths` (see [../README.md](../README.md#profiles)).

| Path | Notes |
|------|-------|
| [simple_wordcount/](simple_wordcount/) | Datagen and print; [profile.yaml](simple_wordcount/profile.yaml) |
| [simple_java_udf/](simple_java_udf/) | PyFlink + Java `ScalarFunction` JAR (Gradle); [profile.yaml](simple_java_udf/profile.yaml) |
| [kind/](kind/) | Kind cluster setup; [kind/README.md](kind/README.md) |
