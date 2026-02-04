val projectVersion: String by project

plugins {
    id("java")
    `maven-publish`
    signing
}

group = "atoma"


val publishableModules = listOf(
    "atoma-api", "atoma-core", "atoma-storage-mongo"
)


allprojects {
    version = projectVersion
    apply(plugin = "java")
    repositories {
        mavenCentral()
    }
    dependencies {
        testImplementation("de.flapdoodle.embed:de.flapdoodle.embed.mongo:4.16.1")
    }
}

configure(subprojects.filter { it.name in publishableModules }) {
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    // 配置源码和文档Jar
    tasks.register<Jar>("sourcesJar") {
        from(sourceSets["main"].allSource)
        archiveClassifier.set("sources")
    }

    tasks.register<Jar>("javadocJar") {
        from(tasks.named("javadoc"))
        archiveClassifier.set("javadoc")
    }

    publishing {
        publications {
            create<MavenPublication>("mavenJava") {
                groupId = project.group.toString()
                artifactId = project.name
                version = project.version.toString()

                from(components["java"])

                artifact(tasks.named("sourcesJar"))
                artifact(tasks.named("javadocJar"))

                pom {
                    name.set(project.name)
                    description.set(project.description ?: "No description")
                }
            }
        }
    }

    signing {
        sign(publishing.publications["mavenJava"])
    }
}