Description
-----------

Implementation of PravegaInputFormat (with wordcount sample)


Compile
-------

mvn clean install

Test
-------

mvn test 

Usage
-----
```
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, "myScope");
        conf.setStrings(PravegaInputFormat.STREAM_NAME, "myStream");
        conf.setStrings(PravegaInputFormat.URI_STRING, "tcp://127.0.0.1:9090");

        // optional
        conf.setBoolean(PravegaInputFormat.DEBUG, true);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, JavaSerializer.class.getName());

        Job job = new Job(conf);
        job.setInputFormatClass(PravegaInputFormat.class);
```

Run Sample
---

mvn exec:java -Dexec.mainClass=io.pravega.sample.WordCount

(Input is read from pravega, and output is written to /tmp/wc/)
