plugins {
    `java-library`
}

val flinkVersion = "1.20.1"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.flink:flink-table-common:$flinkVersion")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.named<Jar>("jar").configure {
    archiveBaseName.set("simple-java-udf")
    archiveVersion.set("1.0.0")
}
