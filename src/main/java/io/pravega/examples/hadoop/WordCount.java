/*
 * Copyright 2017 Dell/EMC
 *
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

package io.pravega.examples.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import io.pravega.hadoop.mapreduce.PravegaInputFormat;

import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.ByteArraySerializer;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, "myScope");
        conf.setStrings(PravegaInputFormat.STREAM_NAME, "myStream");
        conf.setStrings(PravegaInputFormat.URI_STRING, "tcp://127.0.0.1:9090");
        conf.setBoolean(PravegaInputFormat.DEBUG, true);
        //conf.setStrings(PravegaInputFormat.DESERIALIZER, JavaSerializer.class.getName());
        //conf.setStrings(PravegaInputFormat.DESERIALIZER, ByteArraySerializer.class.getName());

        Job job = new Job(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        job.setInputFormatClass(PravegaInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);

        FileOutputFormat.setOutputPath(job, new Path("/tmp/wc/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
