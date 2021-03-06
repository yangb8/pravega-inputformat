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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetadataWritable implements Writable {

    private PravegaInputSplit split;
    private long offset;
    // place holder
    private Long timestamp;

    /**
     * @deprecated Constructor used by Hadoop to init the class through reflection. Do not remove...
     */
    public MetadataWritable() {
    }

    public MetadataWritable(PravegaInputSplit split, long offset) {
        this.split = split;
        this.offset = offset;
        this.timestamp = 0L;
    }

    /**
     * @return offset in key
     */
    public long getOffset() {
        return offset;
    }

    /**
     * @return split in key
     */
    public PravegaInputSplit getSplit() {
        return split;
    }

    /**
     * @return timestamp in key
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        split.write(out);
        WritableUtils.writeVLong(out, getOffset());
        WritableUtils.writeVLong(out, getTimestamp());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        split = new PravegaInputSplit();
        split.readFields(in);
        offset = WritableUtils.readVLong(in);
        timestamp = WritableUtils.readVLong(in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("%s:%s:%s", split.toString(), String.valueOf(getOffset()), Long.toString(getTimestamp()));
    }
}
