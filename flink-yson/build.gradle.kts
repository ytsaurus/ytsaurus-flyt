group = "tech.ytsaurus.flyt.formats.yson"
version = "1.1.1"

plugins {
    id("java-library")
    id("maven-publish")
    id("signing")
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    implementation(project(":flink-yson-fast-adapter"))
    implementation("tech.ytsaurus:yson-tree:1.2.12")

    compileOnly("org.apache.flink:flink-table-common:1.20.1")
    compileOnly("org.apache.flink:flink-format-common:1.20.1")

    testImplementation("org.slf4j:slf4j-log4j12:2.0.17")
    testImplementation("org.apache.flink:flink-table-common:1.20.1")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:3.4.6")

    testCompileOnly("org.projectlombok:lombok:1.18.20")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.20")
}


publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "flink-yson"
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
                name.set("Flink YSON")
                description.set("Flint YSON")
                url.set("https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-yson")
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
                    url.set("https://github.com/ytsaurus/ytsaurus-flyt/tree/main/flink-yson")
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
