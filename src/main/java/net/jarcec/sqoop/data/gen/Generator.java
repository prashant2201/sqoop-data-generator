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
package net.jarcec.sqoop.data.gen;

import net.jarcec.sqoop.data.gen.mr.GeneratorInputFormat;
import net.jarcec.sqoop.data.gen.mr.GeneratorMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Main entry point to the job.
 */
public class Generator {

  public static void main(String []args) throws Exception {
    // ARG 0 output directory
    // ARG 1 comma separated list of types to generate
    // ARG 2 number of files
    // ARG 3 number of records per file

    if(args.length != 4) {
      throw new IllegalArgumentException("Incorrect number of parameters, expected 4, got " + args.length);
    }

    Configuration configuration = new Configuration();

    Path outputPath = new Path(args[0]);
    FileSystem fs = FileSystem.get(configuration);

    fs.delete(outputPath, true);

    configuration.set(Constants.TYPES, args[1]);
    configuration.set(Constants.FILES_COUNT, args[2]);
    configuration.set(Constants.RECORD_COUNT, args[3]);

    Job job = Job.getInstance(configuration);
    job.setJarByClass(Generator.class);

    job.setInputFormatClass(GeneratorInputFormat.class);

    job.setMapperClass(GeneratorMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
  }
}
