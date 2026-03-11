group = "tech.ytsaurus.flyt.connectors.ytsaurus"
version = "1.9.3"

plugins {
    id("com.peterabeles.gversion") version "1.10.3"
    id("java-library")
    id("maven-publish")
    id("signing")
}

dependencies {
    implementation(project(":locks-api"))
    implementation(project(":locks-noop"))
    api(project(":flink-connector-data-metrics"))
    api("tech.ytsaurus:ytsaurus-client:1.2.12")
    implementation(project(":flink-yson-fast-adapter"))
    implementation("jakarta.annotation:jakarta.annotation-api:1.3.5")

    compileOnly("org.apache.flink:flink-core:1.20.1")
    compileOnly("org.apache.flink:flink-runtime:1.20.1")
    compileOnly("org.apache.flink:flink-streaming-java:1.20.1")
    compileOnly("org.apache.flink:flink-table-api-java-bridge:1.20.1")
    compileOnly("org.apache.flink:flink-format-common:1.20.1")
    compileOnly("org.apache.flink:flink-table-common:1.20.1")
    compileOnly("org.apache.flink:flink-table-runtime:1.20.1")
    compileOnly("org.apache.flink:flink-shaded-guava:31.1-jre-17.0")
    compileOnly("org.projectlombok:lombok:1.18.20")
    annotationProcessor("org.projectlombok:lombok:1.18.20")


    testImplementation(project(":locks-api"))
    testImplementation(project(":locks-noop"))

    testImplementation("org.apache.flink:flink-core:1.20.1")
    testImplementation("org.apache.flink:flink-format-common:1.20.1")
    testImplementation("org.apache.flink:flink-table-common:1.20.1")
    testImplementation("org.apache.flink:flink-table-runtime:1.20.1")
    testImplementation("org.apache.flink:flink-shaded-guava:31.1-jre-17.0")

    testImplementation("org.slf4j:slf4j-log4j12:2.0.17")
    testImplementation("org.mockito:mockito-inline:5.1.1")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.23.1")
    testCompileOnly("org.projectlombok:lombok:1.18.20")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.20")
    testImplementation(project(":locks-noop"))
}

sourceSets {
    main {
        java {
            srcDir(layout.buildDirectory.dir("generated-src/version/java"))
        }
    }
}

gversion {
    srcDir = "build/generated-src/version/java"
    classPackage = "tech.ytsaurus.flyt.connectors.ytsaurus"
    className = "YtConnectorInfo"
    annotate = true
}

tasks.compileJava {
    dependsOn(tasks.createVersionFile)
}

tasks.shadowJar {
    mergeServiceFiles()
}

tasks.withType<Checkstyle> {
    exclude("**/tech/ytsaurus/flyt/connectors/ytsaurus/YtConnectorInfo**")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "flink-connector-ytsaurus"
            from(components["java"])

            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("Flint YTsaurus Connector")
                description.set("Flint YTsaurus Connector")
                url.set("https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-connector-ytsaurus")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
            }
        }
    }

    repositories {
        maven {
            val releasesRepoUrl =
                uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://central.sonatype.com/repository/maven-snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

            credentials {
                username = project.properties["ossrhUsername"].toString()
                password = project.properties["ossrhPassword"].toString()
            }
        }
    }
}

signing {
    setRequired({
        !version.toString().endsWith("SNAPSHOT")
    })

    val signingKey: String? by project
    val signingPassword: String? by project

    useInMemoryPgpKeys(signingKey, signingPassword)

    sign(publishing.publications["mavenJava"])
}