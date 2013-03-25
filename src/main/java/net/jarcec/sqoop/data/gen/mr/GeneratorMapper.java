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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Core of the generator process will generate the data as specified on command line.
 */
public class GeneratorMapper extends Mapper<LongWritable, LongWritable, Text, NullWritable> {

  private SecureRandom random;
  private DecimalFormat decimal;
  private SimpleDateFormat date;
  private SimpleDateFormat time;
  private SimpleDateFormat datetime;

  @Override
  protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
    long from = key.get();
    long to = value.get();

    random = new SecureRandom();
    decimal = new DecimalFormat("###.###");
    date = new SimpleDateFormat("yyyy-MM-dd");
    time = new SimpleDateFormat("HH:mm:ss");
    datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    String []types = context.getConfiguration().get(Constants.TYPES).split(",");
    String []values = new String[types.length];

    for(long i = from; i < to; i++) {
      context.progress();

      int y = 0;
      for(String type : types) {

        if("id".equals(type)) {
          values[y] = String.valueOf(i);
        } else if("s50".equals(type)) {
          values[y] = generateString(50);
        } else if("i".equals(type)) {
          values[y] = generateInteger();
        } else if("f".equals(type)) {
          values[y] = generateFloat(250, 31);
        } else if("d".equals(type)) {
          values[y] = generateDate();
        } else if("t".equals(type)) {
          values[y] = generateTime();
        } else if("dt".equals(type)) {
          values[y] = generateDateTime();
        } else if("s255".equals(type)) {
          values[y] = generateString(255);
        } else {
          throw new RuntimeException("Unknown type: " + type);
        }

        y++;
      }

      context.write(new Text(StringUtils.join(values, ",")), NullWritable.get());
    }
  }

  private long getEpoch() {
    // Epoch between 1940 and 2060
    return -946771200000L + (Math.abs(random.nextLong()) % (120L * 365 * 24 * 60 * 60 * 1000));
  }

  private String generateDateTime() {
    return datetime.format(new Date(getEpoch()));
  }

  private String generateTime() {
    return time.format(new Date(getEpoch()));
  }

  private String generateDate() {
    return date.format(new Date(getEpoch()));
  }

  private String generateFloat(long mult, long add) {
    return decimal.format((random.nextFloat() * mult + add) * random.nextFloat());
  }

  private String generateInteger() {
    return String.valueOf(Math.abs(random.nextInt()));
  }

  private String generateString(int i) {
    StringBuilder sb = new StringBuilder();

    while(sb.length() < i) {
      sb.append(new BigInteger(30, random).toString(32));
    }

    return sb.toString().substring(0, i - 1);
  }
}
