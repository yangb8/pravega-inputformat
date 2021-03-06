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

package io.pravega.hadoop.mapreduce;


import io.pravega.client.stream.EventStreamWriter;
import io.pravega.hadoop.mapreduce.PravegaInputFormat;
import io.pravega.hadoop.mapreduce.PravegaInputRecordReader;
import io.pravega.hadoop.mapreduce.utils.IntegerSerializer;
import io.pravega.hadoop.mapreduce.utils.SetupUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class PravegaInputRecordReaderTest {

    private static final String scope = "scope";
    private static final String stream = "stream";
    private static final int numSegments = 1;
    private static final int numEvents = 20;

    /**
     * Setup utility
     */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @Before
    public void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices(this.scope);
        SETUP_UTILS.createTestStream(this.stream, this.numSegments);
        EventStreamWriter<Integer> writer = SETUP_UTILS.getIntegerWriter(this.stream);
        for (int i = 0; i < this.numEvents; i++) {
            CompletableFuture future = writer.writeEvent(i);
            future.get();
        }
    }

    @After
    public void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testInitialize() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, this.scope);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, this.stream);
        conf.setStrings(PravegaInputFormat.URI_STRING, SETUP_UTILS.getControllerUri());
        conf.setStrings(PravegaInputFormat.DESERIALIZER, IntegerSerializer.class.getName());
        Job job = new Job(conf);

        // get an InputSplit
        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat<>();
        List<InputSplit> splits = inputFormat.getSplits(job);
        Assert.assertEquals(this.numSegments, splits.size());

        PravegaInputRecordReader<Integer> r = new PravegaInputRecordReader<>();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        r.initialize(splits.get(0), context);

        for (int i = 0; i < this.numEvents; i++) {
            Assert.assertTrue(r.nextKeyValue());
            Assert.assertEquals(i * 12, r.getCurrentKey().getOffset());
            Assert.assertTrue(i == r.getCurrentValue());
        }
        Assert.assertFalse(r.nextKeyValue());

        r.close();
    }
}
