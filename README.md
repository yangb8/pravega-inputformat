Description
-----------

Implementation of PravegaInputFormat (with wordcount examples)


Build
-------

### Building Pravega

Optional: This step is required only if you want to use a different version of Pravega than is published to maven central.

Install the Pravega client libraries to your local Maven repository:
```
$ git clone https://github.com/pravega/pravega.git
$./gradlew install
```

### Building PravegaInputFormat
```
mvn clean install -DskipTests
```

Test
-------
```
mvn test 
```

Usage
-----
```
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, "myScope");
        conf.setStrings(PravegaInputFormat.STREAM_NAME, "myStream");
        conf.setStrings(PravegaInputFormat.URI_STRING, "tcp://127.0.0.1:9090");

        // optional
        conf.setBoolean(PravegaInputFormat.DEBUG, true);
        // optional, depending on Value class
        conf.setStrings(PravegaInputFormat.DESERIALIZER, JavaSerializer.class.getName());

        Job job = new Job(conf);
        job.setInputFormatClass(PravegaInputFormat.class);
```

Run Examples
---

```
Hadoop (2.8.1)

HADOOP_CLASSPATH=target/hadoop-common-0.0.1.jar HADOOP_USER_CLASSPATH_FIRST=true hadoop jar target/hadoop-common-0.0.1.jar io.pravega.examples.hadoop.WordCount tcp://192.168.0.200:9090 myScope myStream /tmp/wordcount_output
```

```
Spark (2.2.0, commented collect() due to jar version conflict, TODO)

spark-submit --conf spark.driver.userClassPathFirst=true --class io.pravega.examples.spark.WordCount target/hadoop-common-0.0.1.jar tcp://192.168.0.200:9090 myScope myStream
```
