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

import io.pravega.client.segment.impl.Segment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class MetadataWritableTest {

    private static final String scope = "scope";
    private static final String stream = "stream";
    private Segment segment;
    private PravegaInputSplit split;
    private MetadataWritable key;

    @Before
    public void setUp() {
        segment = new Segment(scope, stream, 10);
        split = new PravegaInputSplit(segment, 1, 100);
        key = new MetadataWritable(split, 5L);
    }

    @Test
    public void testMetadataWritable() {
        Assert.assertEquals(5L, key.getOffset());
        Assert.assertEquals(split, key.getSplit());
        Assert.assertEquals(Long.valueOf(0), key.getTimestamp());
    }

    @Test
    public void testMetadataWritableWritable() throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOutput);
        key.write(out);
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOutput.toByteArray()));
        MetadataWritable inKey = new MetadataWritable();
        inKey.readFields(in);
        byteOutput.close();

        Assert.assertEquals(0, key.getSplit().getSegment().compareTo(inKey.getSplit().getSegment()));
        Assert.assertEquals(key.getOffset(), inKey.getOffset());
        Assert.assertEquals(key.getTimestamp(), inKey.getTimestamp());
    }
}
