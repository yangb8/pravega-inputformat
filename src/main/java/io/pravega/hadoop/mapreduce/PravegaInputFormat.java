/*
 * Copyright 2017 DELL/EMC
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

import io.pravega.client.ClientFactory;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PravegaInputFormat<V> extends InputFormat<MetadataWritable, V> {

    public static final String SCOPE_NAME = "pravega.scope";
    public static final String STREAM_NAME = "pravega.stream";
    public static final String URI_STRING = "pravega.uri";
    public static final String DESERIALIZER = "pravega.deserializer";
    public static final String DEBUG = "pravega.debug";

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        List<InputSplit> splits = new ArrayList<InputSplit>();

        final String scopeName = conf.getRaw(PravegaInputFormat.SCOPE_NAME);
        final String streamName = conf.getRaw(PravegaInputFormat.STREAM_NAME);
        final URI controllerURI = URI.create(conf.getRaw(PravegaInputFormat.URI_STRING));
        try (ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI)) {
            BatchClient batchClient = clientFactory.createBatchClient();

            for (Iterator<SegmentInfo> iter = batchClient.listSegments(new StreamImpl(scopeName, streamName)); iter.hasNext(); ) {
                SegmentInfo segInfo = iter.next();
                Segment segment = segInfo.getSegment();
                PravegaInputSplit split = new PravegaInputSplit(segment, 0, segInfo.getLength());
                splits.add(split);
            }
        }
        return splits;
    }

    @Override
    public RecordReader<MetadataWritable, V> createRecordReader(
            InputSplit inputSplit,
            TaskAttemptContext context
    ) throws IOException, InterruptedException {
        return new PravegaInputRecordReader<V>();
    }
}
