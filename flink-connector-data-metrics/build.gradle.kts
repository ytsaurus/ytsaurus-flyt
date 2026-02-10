group = "tech.ytsaurus.flyt.connectors.datametrics"
version = "1.0.0"

plugins {
    id("java-library")
}

dependencies {
    compileOnly("org.apache.flink:flink-table-common:1.20.1")
    compileOnly("org.apache.flink:flink-streaming-java:1.20.1")
    compileOnly("org.apache.flink:flink-metrics-core:1.20.1")

    compileOnly("org.projectlombok:lombok:1.18.20")
    annotationProcessor("org.projectlombok:lombok:1.18.20")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-inline:5.1.1")
    testImplementation("org.apache.flink:flink-metrics-core:1.20.1")
    testImplementation("org.apache.flink:flink-table-api-java:1.20.1")
    testImplementation("org.apache.flink:flink-table-runtime:1.20.1")
    testCompileOnly("org.projectlombok:lombok:1.18.20")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.20")
}
