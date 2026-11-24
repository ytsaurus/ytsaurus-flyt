group = "tech.ytsaurus.flyt.connectors.ytsaurus"
version = "1.13.0"

plugins {
    id("com.peterabeles.gversion") version "1.10.3"
    id("java-library")
    id("maven-publish")
    id("signing")
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    implementation(project(":locks-api"))
    implementation(project(":locks-noop"))
    api(project(":flink-connector-data-metrics"))
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.4")
    api("tech.ytsaurus:ytsaurus-client:1.2.12")
    implementation(project(":flink-yson-fast-adapter"))
    implementation("jakarta.annotation:jakarta.annotation-api:1.3.5")
    implementation("com.github.luben:zstd-jni:1.5.2-5")

    compileOnly("org.apache.flink:flink-connector-base:1.20.1")
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

    testImplementation(project(":flink-yson"))

    testImplementation("org.apache.flink:flink-connector-base:1.20.1")
    testImplementation("org.apache.flink:flink-core:1.20.1")
    testImplementation("org.apache.flink:flink-connector-test-utils:1.20.1")
    testImplementation("org.apache.flink:flink-format-common:1.20.1")
    testImplementation("org.apache.flink:flink-table-common:1.20.1")
    testImplementation("org.apache.flink:flink-table-runtime:1.20.1")
    testImplementation("org.apache.flink:flink-shaded-guava:31.1-jre-17.0")
    testRuntimeOnly("org.apache.flink:flink-json:1.20.1")

    testImplementation("org.slf4j:slf4j-log4j12:2.0.17")
    testImplementation("org.mockito:mockito-inline:5.1.1")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.23.1")
    testCompileOnly("org.projectlombok:lombok:1.18.20")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.20")
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
    relocate("com.google.protobuf", "tech.ytsaurus.flyt.connectors.ytsaurus.shaded.com.google.protobuf")
    // User protobuf jars can also contain NYT.Extension. Isolate the connector's generated classes
    // together with protobuf to avoid mixing shaded and unshaded descriptor types.
    relocate("NYT", "tech.ytsaurus.flyt.connectors.ytsaurus.shaded.NYT")
}

tasks.withType<Checkstyle> {
    exclude("**/tech/ytsaurus/flyt/connectors/ytsaurus/YtConnectorInfo**")
}

val connectorGroup = project.group.toString()
val connectorArtifact = "flink-connector-ytsaurus"
val connectorVersion = project.version.toString()
val connectorReadme = layout.projectDirectory.file("README.md")

val checkReadmeVersion by tasks.registering {
    group = "verification"
    description = "Checks this connector's Maven and Gradle README examples against the project version."
    inputs.file(connectorReadme)
    inputs.property("connectorVersion", connectorVersion)

    doLast {
        val readme = connectorReadme.asFile.readText()
        val coordinate = "$connectorGroup:$connectorArtifact"
        fun checkVersion(example: String, found: String?) {
            if (found != connectorVersion) {
                throw GradleException("README.md $example for $coordinate: expected version " +
                        "$connectorVersion, found ${found?.takeIf { it.isNotBlank() } ?: "<missing>"}.")
            }
        }
        fun xmlValues(dependency: String, element: String): List<String> =
            Regex("""<$element\s*>([^<]*)</$element\s*>""").findAll(dependency)
                .map { it.groupValues[1].trim() }.toList()

        val mavenExamples = Regex("""<dependency\s*>(.*?)</dependency\s*>""", RegexOption.DOT_MATCHES_ALL)
            .findAll(readme).map { it.groupValues[1] }
            .filter { connectorGroup in xmlValues(it, "groupId") && connectorArtifact in xmlValues(it, "artifactId") }
            .toList()
        if (mavenExamples.isEmpty()) {
            throw GradleException("README.md: expected a Maven dependency example for $coordinate:$connectorVersion, found none.")
        }
        mavenExamples.forEachIndexed { index, dependency ->
            val versions = xmlValues(dependency, "version")
            if (versions.isEmpty()) checkVersion("Maven example ${index + 1}", null)
            versions.forEach { checkVersion("Maven example ${index + 1}", it) }
        }

        val gradleExamples = Regex(
            """(["'])\s*${Regex.escape(connectorGroup)}\s*:\s*${Regex.escape(connectorArtifact)}\s*(?::([^"']*))?\1"""
        ).findAll(readme).toList()
        if (gradleExamples.isEmpty()) {
            throw GradleException("README.md: expected a Gradle dependency example for $coordinate:$connectorVersion, found none.")
        }
        gradleExamples.forEachIndexed { index, example ->
            checkVersion("Gradle example ${index + 1}", example.groupValues[2].substringBefore(':').substringBefore('@').trim())
        }
    }
}

tasks.check {
    dependsOn(checkReadmeVersion)
}

val releaseVersion = providers.gradleProperty("releaseVersion")
tasks.register("checkReleaseVersion") {
    group = "verification"
    description = "Checks the required -PreleaseVersion against the project and README versions."
    dependsOn(checkReadmeVersion)
    inputs.property("connectorVersion", connectorVersion)
    inputs.property("releaseVersion", releaseVersion.orElse(""))

    doLast {
        val requestedVersion = releaseVersion.orNull
        if (requestedVersion.isNullOrBlank()) {
            throw GradleException("Missing -PreleaseVersion: expected $connectorVersion for $connectorArtifact.")
        }
        if (requestedVersion != connectorVersion) {
            throw GradleException("Release version mismatch for $connectorArtifact: " +
                    "expected $connectorVersion (project.version), found $requestedVersion (-PreleaseVersion).")
        }
    }
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
                name.set("Flink YTsaurus Connector")
                description.set("Apache Flink connector for reading and writing YTsaurus dynamic tables and queues.")
                url.set("https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-connector-ytsaurus")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("DanilSabirov")
                        name.set("Danil Sabirov")
                        email.set("danil.sabirov.work@gmail.com")
                        organization.set("Yandex LLC")
                        organizationUrl.set("https://yandex.ru/company")
                    }
                    developer {
                        id.set("TheSeems")
                        name.set("Alexey Akhundov")
                        email.set("me@theseems.ru")
                        organization.set("Yandex LLC")
                        organizationUrl.set("https://yandex.ru/company")

                    }
                    developer {
                        id.set("Yahor")
                        name.set("Yahor Burakevich")
                        email.set("egor.burakevich93@gmail.com")
                        organization.set("Yandex LLC")
                        organizationUrl.set("https://yandex.ru/company")

                    }
                    developer {
                        id.set("vovapraded")
                        name.set("Vladimir Praded")
                        email.set("vovatv2017@gmail.com")
                        organization.set("Yandex LLC")
                        organizationUrl.set("https://yandex.ru/company")

                    }
                    developer {
                        id.set("smiralexan")
                        name.set("Alexander Smirnov")
                        email.set("smiralexan@gmail.com")
                        organization.set("Yandex LLC")
                        organizationUrl.set("https://yandex.ru/company")

                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/ytsaurus/ytsaurus-flyt.git")
                    developerConnection.set("scm:git:ssh://github.com/ytsaurus/ytsaurus-flyt.git")
                    url.set("https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-connector-ytsaurus")
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
