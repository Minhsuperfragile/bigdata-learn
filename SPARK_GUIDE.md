# Spark Development Guide (Low-Memory & Java 21)

This guide documents how to learn and develop with **Apache Spark in Java** on this local environment without heavy memory consumption or large datasets.

---

## 1. Overview & Architecture

Even though this workspace was initially configured for Hadoop MapReduce, Spark seamlessly reuses the exact same storage and cluster layers:

* **Storage (HDFS):** Accessible from your local Java code at `hdfs://localhost:9000/`.
* **Resource Negotiator (YARN):** Accessible at `localhost:8088` (optional for local learning).
* **Compute Engine:** Apache Spark (runs locally in Java, replacing MapReduce for compute).

```text
┌────────────────────────────────────────────────────────┐
│               Spark Java Application                   │
│   (Local Mode: master("local[*]") | JVM Heap < 512MB)  │
└──────────────────────────┬─────────────────────────────┘
                           │ Reads / Writes
                           ▼
┌────────────────────────────────────────────────────────┐
│                  HDFS in Docker                        │
│          (hdfs://localhost:9000/input/...)             │
└────────────────────────────────────────────────────────┘
```

---

## 2. Bypassing the 1 GB+ RAM Requirement

Spark's default 1 GB requirement (`spark.driver.memory=1g`, `spark.executor.memory=1g`) is an arbitrary production default for enterprise clusters. For learning and development on toy data, you can run Spark with **256 MB to 512 MB of RAM**.

### Approach A: Local Mode in Java (Recommended)
Running Spark in local mode runs everything inside a single JVM process on Windows:
* **No separate executor containers:** No container scheduling overhead.
* **Low heap limit:** You can set standard JVM flags (e.g., `-Xmx384m` or `-Xmx512m`).
* **Bypassing the 471 MB Heap Check:** Spark has a built-in safety guard that throws `System memory must be at least 471859200`. You can safely bypass this for small data using:
  ```java
  .config("spark.testing.memory", "2147483648") // or "471859200"
  ```

### Approach B: Submitting to YARN with Low Memory
If you ever test submitting to YARN inside Docker (`spark-submit --master yarn`), your `config` file already has physical/virtual memory enforcement disabled (`pmem-check-enabled=false`, `vmem-check-enabled=false`).

To submit with low memory:
```bash
spark-submit --master yarn \
  --conf spark.driver.memory=256m \
  --conf spark.executor.memory=256m \
  --conf spark.yarn.am.memory=256m \
  target/your-app.jar
```

---

## 3. Java 21 Compatibility (`--add-opens`)

The Windows host runs **JDK 21**. Because Java 9+ enforces strong module encapsulation, Spark 3.5.x requires JVM arguments to access internal memory buffers and serialization tools.

### Required JVM Arguments
```text
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```

---

## 4. Minimal Java Code Example

Save as `src/main/java/com/example/SparkDemo.java`:

```java
package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;

public class SparkDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("LowMemorySparkDemo")
            .master("local[*]") // Run locally across host CPU cores
            // Low-memory configurations
            .config("spark.driver.memory", "256m")
            .config("spark.executor.memory", "256m")
            .config("spark.testing.memory", "2147483648") // Bypass 471MB guard
            .config("spark.sql.shuffle.partitions", "2")  // Default is 200; reduce for toy data
            .getOrCreate();

        // 1. Process in-memory small data (no dataset file needed)
        Dataset<Row> df = spark.read().json(
            spark.createDataset(
                Arrays.asList(
                    "{\"name\": \"Alice\", \"role\": \"Data Engineer\"}",
                    "{\"name\": \"Bob\", \"role\": \"ML Engineer\"}"
                ),
                org.apache.spark.sql.Encoders.STRING()
            )
        );
        df.show();
        df.printSchema();

        // 2. Read from running Docker HDFS (when needed):
        // Dataset<Row> hdfsData = spark.read().textFile("hdfs://localhost:9000/input/sample.txt");
        // hdfsData.show();

        spark.stop();
    }
}
```

---

## 5. Maven Dependencies (`pom.xml`)

Add the following to your Maven `pom.xml`:

```xml
<dependencies>
  <!-- Spark Core & SQL -->
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.13</artifactId>
    <version>3.5.3</version>
  </dependency>
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.13</artifactId>
    <version>3.5.3</version>
  </dependency>
</dependencies>

<build>
  <plugins>
    <!-- Compiler configured for modern Java -->
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-compiler-plugin</artifactId>
      <version>3.11.0</version>
      <configuration>
        <release>21</release>
      </configuration>
    </plugin>

    <!-- Exec plugin configured to fork Java with low memory & add-opens -->
    <plugin>
      <groupId>org.codehaus.mojo</groupId>
      <artifactId>exec-maven-plugin</artifactId>
      <version>3.1.0</version>
      <configuration>
        <executable>java</executable>
        <arguments>
          <argument>-Xmx512m</argument>
          <argument>--add-opens=java.base/java.lang=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.lang.invoke=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.lang.reflect=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.io=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.net=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.nio=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.util=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.util.concurrent=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/sun.nio.ch=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/sun.nio.cs=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/sun.security.action=ALL-UNNAMED</argument>
          <argument>--add-opens=java.base/sun.util.calendar=ALL-UNNAMED</argument>
          <argument>--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED</argument>
          <argument>-classpath</argument>
          <classpath/>
          <argument>com.example.SparkDemo</argument>
        </arguments>
      </configuration>
    </plugin>
  </plugins>
</build>
```

---

## 6. VS Code Debugging (`.vscode/launch.json`)

To run and debug Spark Java applications directly in VS Code:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "java",
      "name": "Run Spark Java (Low Memory)",
      "request": "launch",
      "mainClass": "com.example.SparkDemo",
      "projectName": "hadoop-mapreduce-app",
      "vmArgs": "-Xmx512m --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    }
  ]
}
```
