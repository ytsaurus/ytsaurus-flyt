group = "tech.ytsaurus.flyt.locks.noop"
version = "1.0.0"

dependencies {
    implementation(project(":locks-api"))
    compileOnly("org.apache.flink:flink-core:1.20.1")
    compileOnly("org.projectlombok:lombok:1.18.20")
    annotationProcessor("org.projectlombok:lombok:1.18.20")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}
