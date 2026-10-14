# Hadoop Dev Workspace

Local single-node Hadoop 3 environment running on Windows 11 via Docker Compose, paired with local Java 8/11 development in VS Code. Designed for zero-overhead ROM/disk consumption and seamless MapReduce execution.

---

## Architecture & Setup Overview

* **Host Machine:** Windows 11 (`C:\Users\SFX16-51G\Documents\Python\hadoop-workspace`)
* **Container Runtime:** Docker Desktop (WSL 2 backend, containerd v2+ compatible)
* **Base Image:** `apache/hadoop:3.3.6` (OCI compliant, single shared image for all services)
* **Execution Model:** Pseudo-distributed cluster via Docker Compose (shared disk volume)
* **Dev Workflow:** Write & build locally on Windows (Maven/VS Code) $\rightarrow$ output `.jar` directly into mounted volume $\rightarrow$ execute inside container.

---

## Directory Structure

```text
hadoop-workspace/
├── config                     # Unified Hadoop XML/environment configurations
├── docker-compose.yml         # Container definitions (NameNode, DataNode, RM, NM)
├── README.md                  # System & agent setup documentation
├── SPARK_GUIDE.md             # Spark learning guide (Java, low-memory, & HDFS integration)
├── .vscode/
│   └── launch.json            # VS Code 1-click execution for Spark & MapReduce
├── hadoop-mapreduce-app/      # Primary Java Maven project (MapReduce)
│   ├── pom.xml                # Dependencies with <scope>provided</scope>
│   └── src/
│       └── main/java/com/example/
│           └── WordCount.java # MapReduce driver, mapper, and reducer
└── RDD-spark/                 # Spark Java project (RDD API, low-memory, Java 21)
    ├── pom.xml                # Spark 3.5.3 dependencies & fork exec plugin
    └── src/
        └── main/java/com/example/rdd/
            └── WordCountRdd.java # RDD transformations (parallelize, flatMap, reduceByKey)
```

---

## Spark Learning
* Guide: [SPARK_GUIDE.md](file:///c:/Users/SFX16-51G/Documents/Python/hadoop-workspace/SPARK_GUIDE.md) for running Spark on this setup with minimal RAM (< 512 MB).
* Project: [RDD-spark](file:///c:/Users/SFX16-51G/Documents/Python/hadoop-workspace/RDD-spark) is pre-configured and tested for Java 21. Run with `mvn exec:exec` or via VS Code Run.