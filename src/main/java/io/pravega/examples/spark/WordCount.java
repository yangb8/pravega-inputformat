/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.examples.spark;

import io.pravega.hadoop.mapreduce.MetadataWritable;
import io.pravega.hadoop.mapreduce.PravegaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;


public final class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 3) {
            System.err.println("Usage: WordCount <url> <scope> <stream>");
            System.exit(2);
        }

        conf.setStrings(PravegaInputFormat.URI_STRING, remainingArgs[0]);
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, remainingArgs[1]);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, remainingArgs[2]);
        conf.setBoolean(PravegaInputFormat.DEBUG, true);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());

        JavaPairRDD<MetadataWritable, String> lines = sc.newAPIHadoopRDD(conf, PravegaInputFormat.class, MetadataWritable.class, String.class);
        JavaRDD<String> words = lines.map(x -> x._2).flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		/*
         * TODO: comment out now, because there is jar version conflict for netty-all
		 *	prevega client uses io.netty:netty-all:4.1.15.Final, but spark uses io.netty:netty-all:4.0.x.Final
		*/
        //System.out.println(counts.collect());
    }
}