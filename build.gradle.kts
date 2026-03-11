plugins {
    id("checkstyle")
    id("java")
    id("com.gradleup.shadow") version "8.3.9"
}

buildscript {
    repositories {
        gradlePluginPortal()
    }
}

allprojects {
    group = "tech.ytsaurus.flyt"

    apply(plugin = "java")
    apply(plugin = "com.gradleup.shadow")

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    repositories {
        mavenLocal()
        mavenCentral()
        maven {
            url = uri("https://jcenter.bintray.com")
        }
    }

    configurations {
        testImplementation.get().extendsFrom(shadow.get())
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    tasks.withType<Javadoc> {
        options {
            (this as StandardJavadocDocletOptions).apply {
                encoding = "UTF-8"
                charSet = "UTF-8"
                docEncoding = "UTF-8"
                tags("apiNote:a:API Note:")
                tags("implNote:a:Implementation:")
            }
        }
    }

    tasks.assemble {
        dependsOn("shadowJar")
    }

    tasks.test {
        useJUnitPlatform()
    }
}

tasks.jar {
    isEnabled = false
}
tasks.shadowJar {
    isEnabled = false
}
