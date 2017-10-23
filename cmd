mvn clean install
mvn test
rm -rf /tmp/wc/ && mvn exec:java -Dexec.mainClass=io.pravega.sample.WordCount
