group = "tech.ytsaurus.flyt.formats.yson"
version = "1.1.0"

dependencies {
    implementation(project(":flink-yson-fast-adapter"))
    implementation("tech.ytsaurus:yson-tree:1.2.12")
    implementation("org.apache.flink:flink-table-common:1.20.1")

    testImplementation("org.apache.flink:flink-table-common:1.20.1")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:3.4.6")

    testCompileOnly("org.projectlombok:lombok:1.18.20")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.20")
}
