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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Generator's split containing from and to value.
 *
 * Number of lines can be calculated by evaluating (to - from).
 */
public class GeneratorSplit extends InputSplit implements Writable {

  long from;
  long to;

  public GeneratorSplit() {
  }

  public GeneratorSplit(long from, long to) {
    this.from = from;
    this.to = to;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 1;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(from);
    dataOutput.writeLong(to);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    from = dataInput.readLong();
    to = dataInput.readLong();
  }
}
