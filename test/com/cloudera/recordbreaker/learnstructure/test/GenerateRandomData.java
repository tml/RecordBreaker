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
package com.cloudera.recordbreaker.learnstructure.test;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.Utf8;

import org.codehaus.jackson.JsonGenerator;

import com.cloudera.recordbreaker.schemadict.TestRecord;
import com.cloudera.recordbreaker.schemadict.SchemaSuggest;

/**
 * @author mjc
 */
public class GenerateRandomData {
  Schema schema;
  Random r = new Random();
  public GenerateRandomData() {
    this.schema = schema;
  }

  Object generateData(Schema s) {
    Schema.Type stype = s.getType();
    if (stype == Schema.Type.ARRAY) {
      Schema arrayS = s.getElementType();
      int numElts = 1 + r.nextInt(100);
      GenericData.Array result = new GenericData.Array(numElts, arrayS);
      for (int i = 0; i < numElts; i++) {
        result.add(generateData(arrayS));
      }
      return arrayS;
    } else if (stype == Schema.Type.BOOLEAN) {
      return r.nextInt(2) == 0 ? new Boolean(true) : new Boolean(false);
    } else if (stype == Schema.Type.BYTES) {
      return ByteBuffer.wrap(new byte[16]);
    } else if (stype == Schema.Type.DOUBLE) {
      return new Double(r.nextDouble());
    } else if (stype == Schema.Type.ENUM) {
      List<String> symbols = s.getEnumSymbols();
      return symbols.get(r.nextInt(symbols.size()));
    } else if (stype == Schema.Type.FIXED) {
      return new GenericData.Fixed(s, new byte[16]);
    } else if (stype == Schema.Type.FLOAT) {
      return new Float(r.nextFloat());
    } else if (stype == Schema.Type.INT) {
      return new Integer(r.nextInt());
    } else if (stype == Schema.Type.LONG) {
      return new Long(r.nextLong());
    } else if (stype == Schema.Type.MAP) {
      HashMap<Utf8, Object> result = new HashMap<Utf8, Object>();
      Schema valType = s.getValueType();
      int maxElts = 1 + r.nextInt(100);
      for (int i = 0; i < maxElts; i++) {
        result.put(new Utf8("label-" + i), generateData(valType));
      }
      return result;
    } else if (stype == Schema.Type.NULL) {
      return null;
    } else if (stype == Schema.Type.RECORD) {
      GenericData.Record result = new GenericData.Record(s);
      for (Schema.Field f: s.getFields()) {
        result.put(f.name(), generateData(f.schema()));
      }
      return result;
    } else if (stype == Schema.Type.STRING) {
      return new Utf8("Rand-" + r.nextInt());
    } else if (stype == Schema.Type.UNION) {
      List<Schema> types = s.getTypes();
      return generateData(types.get(r.nextInt(types.size())));
    }
    return null;
  }

  /**
   */
  public void generateData(boolean encodeJson, File outfile, int numRecords) throws IOException {
    Schema schema = ReflectData.get().getSchema(TestRecord.class);
    DatumWriter dout = new ReflectDatumWriter(schema);

    if (encodeJson) {
      BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outfile));
      try {
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, (OutputStream) out);
        for (int i = 0; i < numRecords; i++) {
          TestRecord tr = new TestRecord();
          dout.write(tr, encoder);
        }
        encoder.flush();
      } finally {
        out.close();
      }
    } else {
      DataFileWriter out = new DataFileWriter(dout);
      try {
        out.create(schema, outfile);
        for (int i = 0; i < numRecords; i++) {
          TestRecord tr = new TestRecord();
          out.append(tr);
        }
      } finally {
        out.close();
      }
    }
  }

  /**
   */
  public static void main(String argv[]) throws IOException {
    CommandLine cmd = null;
    Options options = new Options();
    options.addOption("?", false, "Help for command-line");
    options.addOption("n", true, "Number elts to emit");

    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException pe) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("GenerateRandomData", options, true);
      System.exit(-1);
    }

    if (cmd.hasOption("?")) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("GenerateRandomData", options, true);
      System.exit(0);
    }

    int numToEmit = 100;
    if (cmd.hasOption("n")) {
      try {
        numToEmit = Integer.parseInt(cmd.getOptionValue("n"));
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
    }

    String[] argArray = cmd.getArgs();
    if (argArray.length == 0) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("GenerateRandomData", options, true);
      System.exit(0);
    }
    File inputSchemaFile = new File(argArray[0]).getCanonicalFile();
    File outputDataFile = new File(argArray[1]).getCanonicalFile();
    if (outputDataFile.exists()) {
      System.err.println("Output file already exists: " + outputDataFile.getCanonicalPath());
      System.exit(0);
    }

    GenerateRandomData grd = new GenerateRandomData();
    Schema schema = Schema.parse(inputSchemaFile);

    GenericDatumWriter datum = new GenericDatumWriter(schema);
    DataFileWriter out = new DataFileWriter(datum);
    out.create(schema, outputDataFile);
    try {
      for (int i = 0; i < numToEmit; i++) {
        out.append((GenericData.Record) grd.generateData(schema));
      }
    } finally {
      out.close();
    }
  }
}
