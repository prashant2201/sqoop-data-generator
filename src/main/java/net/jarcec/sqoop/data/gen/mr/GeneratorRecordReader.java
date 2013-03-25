/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.jarcec.sqoop.data.gen.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Record reader for generator input format.
 */
public class GeneratorRecordReader extends RecordReader {

  GeneratorSplit split;
  boolean read;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    split = (GeneratorSplit) inputSplit;
    read = false;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if(read) {
      return false;
    }

    read = true;
    return true;
  }

  @Override
  public Object getCurrentKey() throws IOException, InterruptedException {
    return new LongWritable(split.from);
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    return new LongWritable(split.to);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if(read) {
      return 1F;
    } else {
      return 0F;
    }
  }

  @Override
  public void close() throws IOException {
  }
}
