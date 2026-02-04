plugins {
    id("java")
    id("java-library")
    id("me.champeau.jmh") version "0.7.2"

}

dependencies {
    api(project(":atoma-core"))
    api(project(":atoma-storage-mongo"))
    implementation(lib.mongodriver)
    implementation("org.openjdk.jmh:jmh-core:1.37")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")

    // For JMH benchmarks using test containers
    implementation(lib.testcontainers.mongodb)

    // Add all necessary dependencies for the JMH source set
    jmhImplementation(project(":atoma-core"))
    jmhImplementation(project(":atoma-storage-mongo"))
    jmhImplementation(lib.mongodriver)
    jmhImplementation(lib.testcontainers.mongodb)
}

jmh {
    resultFormat = "json"
    resultsFile = project.file("${buildDir}/reports/jmh/jmh-result.json")
    includes = listOf(
        "atoma.benchmark.MutexLockBenchmark",
        "atoma.benchmark.CountDownLatchBenchmark",
        "atoma.benchmark.CyclicBarrierBenchmark",
        "atoma.benchmark.ReadWriteLockBenchmark",
        "atoma.benchmark.SemaphoreBenchmark"
    )
}
