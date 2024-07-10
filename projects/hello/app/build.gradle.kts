plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    alias(libs.plugins.jvm)
    // Apply the application plugin to add support for building a CLI application in Java.
    application
    // NOTE: uber jar
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven {
        name = "apache.snapshots"
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
        mavenContent {
            snapshotsOnly()
        }
    }
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // This dependency is used by the application.
    implementation(libs.guava)
    implementation(kotlin("stdlib"))

    // NOTE: Kafka
    implementation("org.apache.kafka:kafka-streams:3.5.1")
    implementation("org.apache.kafka:kafka-clients:3.5.1")

    // NOTE: 로그
    implementation("org.slf4j:slf4j-api:1.7.32")
    implementation("org.apache.logging.log4j:log4j-core:2.20.0")
    implementation("org.apache.logging.log4j:log4j-api:2.20.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.20.0")

    // NOTE: 직렬화
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.3")
    implementation("com.fasterxml.jackson.core:jackson-core:2.13.3")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.3")

    // NOTE: 통신
    implementation("com.squareup.retrofit2:retrofit:2.9.0")
    implementation("com.squareup.retrofit2:converter-gson:2.9.0")

    // NOTE: Flink
    implementation("org.apache.flink:flink-java:1.17.2") // https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-java/1.17.2
    implementation("org.apache.flink:flink-streaming-java:1.17.2")
    implementation("org.apache.flink:flink-connector-kafka:1.17.2")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(8)
    }
}

application {
    // Define the main class for the application.
    mainClass = "com.ecubelabs.AppKt"
}

tasks {
    withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    shadowJar {
        archiveClassifier.set("")
        mergeServiceFiles()
    }

    jar {1
        manifest {
            attributes(
                    "Main-Class" to "com.ecubelabs.AppKt"
            )
        }
    }

    distZip {
        dependsOn(shadowJar)
    }

    distTar {
        dependsOn(shadowJar)
    }

    startScripts {
        dependsOn(shadowJar)
    }

    build {
        dependsOn(shadowJar)
    }

    startShadowScripts {
        dependsOn(jar)
    }
}