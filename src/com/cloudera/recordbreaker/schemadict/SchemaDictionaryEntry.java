/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.recordbreaker.schemadict;

import java.io.File;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;

import org.apache.avro.Schema;

/***********************************************************
 * A SchemaDictionaryEntry is a record containing a schema, a
 * statistical-summary, and a user-written info string.
 *
 * @author mjc
 ***********************************************************/
public class SchemaDictionaryEntry {
  public static final String SUMMARY_ENDING = ".sss";
  public static final String SCHEMA_ENDING = ".schema";
  public static final String INFO_ENDING = ".info";

  SchemaStatisticalSummary summary;
  Schema schema;
  String info;

  /**
   */
  public SchemaDictionaryEntry() {
  }

  /**
   * Load a dictionary entry from disk.
   */
  public SchemaDictionaryEntry(File avroFile, String infoText) throws IOException {
    // Generate schema & summary data from the avro file
    this.summary = new SchemaStatisticalSummary("dictionary entry: " + infoText);
    this.schema = summary.createSummaryFromData(avroFile);
    this.info = infoText;
  }

  /**
   * Save dictionary entry to disk.
   */
  public void saveDictionaryEntry(File dir, String fileRoot) throws IOException {
    // Store computed info to the local dir (we don't store the original data, just the summary)
    File summaryFile = new File(dir, fileRoot + SUMMARY_ENDING);
    File schemaFile = new File(dir, fileRoot + SCHEMA_ENDING);
    File infoFile = new File(dir, fileRoot + INFO_ENDING);

    DataOutputStream out = new DataOutputStream(new FileOutputStream(summaryFile));
    try {
      this.summary.write(out);
    } finally {
      out.close();
    } 

    OutputStreamWriter out2 = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(schemaFile)));
    try {
      out2.write(schema.toString(true));
    } finally {
      out2.close();
    }         

    out = new DataOutputStream(new FileOutputStream(infoFile));
    try {
      out.writeUTF(this.info);
    } finally {
      out.close();
    }
  }

  /**
   * Load dictionary entry from disk.
   */
  public void loadDictionaryEntry(File dir, String fileRoot) throws IOException {
    File summaryFile = new File(dir, fileRoot + SUMMARY_ENDING);
    File schemaFile = new File(dir, fileRoot + SCHEMA_ENDING);
    File infoTextFile = new File(dir, fileRoot + INFO_ENDING);

    this.summary = new SchemaStatisticalSummary();
    DataInputStream in = new DataInputStream(new FileInputStream(summaryFile));
    try {
      summary.readFields(in);
    } finally {
      in.close();
    }

    this.schema = Schema.parse(schemaFile);

    in = new DataInputStream(new FileInputStream(infoTextFile));
    StringBuffer buf = new StringBuffer();
    try {
      String s = null;
      while ((s = in.readLine()) != null) {
        buf.append(s);
        buf.append("\n");
      }
      this.info = buf.toString().trim();
    } finally {
      in.close();
    }
  }

  ////////////////////////////////////////////////
  // Accessor methods
  ////////////////////////////////////////////////
  public SchemaStatisticalSummary getSummary() {
    return summary;
  }
  public Schema getSchema() {
    return schema;
  }
  public String getInfo() {
    return info;
  }
  public String toString() {
    return "Info: " + getInfo() + ", schema: " + getSchema();
  }
}
