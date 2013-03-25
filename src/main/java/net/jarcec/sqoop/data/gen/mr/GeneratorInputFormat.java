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

import net.jarcec.sqoop.data.gen.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Input format that will generate predefined number of input splits where each
 * split will generated predefined number of lines.
 */
public class GeneratorInputFormat extends InputFormat {
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    Configuration configuration = jobContext.getConfiguration();

    long files = configuration.getLong(Constants.FILES_COUNT, 0);
    long records = configuration.getLong(Constants.RECORD_COUNT, 0);

    List<InputSplit> splits = new LinkedList<InputSplit>();

    long next = 1;
    for(int i = 0; i < files; i++) {
      splits.add(new GeneratorSplit(next, next + records));
      next += records;
    }

    return splits;
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new GeneratorRecordReader();
  }
}
