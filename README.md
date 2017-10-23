Description
-----------

Implementation of PravegaInputFormat (with wordcount sample)

Input is read from pravega, and output is written to /tmp/wordcount/out*.

Compile
-------

mvn clean install

Test
-------

mvn test 

Usage
-----
```
        Configuration conf = getConf();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, "myScope");
        conf.setStrings(PravegaInputFormat.STREAM_NAME, "myStream");
        conf.setStrings(PravegaInputFormat.URI_STRING, "tcp://127.0.0.1:9090");
        // optional start/end time, and deserializer
        conf.setStrings(PravegaInputFormat.START_TIME, <Long: in milliseconds>);
        conf.setStrings(PravegaInputFormat.END_TIME, <Long: in milliseconds>;
        conf.setStrings(PravegaInputFormat.DESERIALIZER, JavaSerializer.class.getName());
        Job job = new Job(conf);
        job.setInputFormatClass(PravegaInputFormat.class);
```

Run
---

mvn exec:java -Dexec.mainClass=io.pravega.wordcount.App
