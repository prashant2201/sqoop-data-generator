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

/**
 * Constants that will be used in mapreduce configuration.
 */
public class Constants {

  /**
   * Comma separated list of column types that should be generated.
   */
  public static final String TYPES = "net.jarcec.sqoop.generator.types";

  /**
   * Number of files that should be generated.
   */
  public static final String FILES_COUNT = "net.jarcec.sqoop.generator.files";

  /**
   * Number of lines in each file that should be generated.
   */
  public static final String RECORD_COUNT = "net.jarcec.sqoop.generator.records";
}