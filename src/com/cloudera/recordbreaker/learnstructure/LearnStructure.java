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
package com.cloudera.recordbreaker.learnstructure;

import java.io.*;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;

/*********************************************************
 * LearnStructure is the main file for figuring out pattern-extractors and schemas for a text file.
 *
 * This code operates on a raw text file and emits the extractors/schemas.  The user
 * may decide to remove certain extractors/schemas if they only apply to a tiny number of
 * potential lines in the target text file.
 *
 *********************************************************/
public class LearnStructure {
  static String SCHEMA_FILENAME = "schema.json";
  static String JSONDATA_FILENAME = "data.avro.json";
  static String DATA_FILENAME = "data.avro";
  static String PARSER_FILENAME = "parser.dat";

  /**
   *
   */
  public void inferRecordFormat(File f, File outdir, boolean emitAvro) throws IOException {
    File schemaFile = new File(outdir, SCHEMA_FILENAME);
    File jsonDataFile = new File(outdir, JSONDATA_FILENAME);
    File dataFile = new File(outdir, DATA_FILENAME);
    File parseTreeFile = new File(outdir, PARSER_FILENAME);

    // Store parse errors and results
    List<Integer> unparseableLineNos = new ArrayList<Integer>();
    List<String> unparseableStrs = new ArrayList<String>();
    List<Integer> parseableLineNos = new ArrayList<Integer>();
    List<List<Token.AbstractToken>> allChunks = new ArrayList<List<Token.AbstractToken>>();

    //
    // Transform the text into a list of "chunks".  A single chunk corresponds to a line of text.  A chunk is a list of Tokens.
    //
    long startRead = System.currentTimeMillis();
    BufferedReader in = new BufferedReader(new FileReader(f));
    try {
      String s = in.readLine();
      int lineno = 0;
      while (s != null) {
        List<Token.AbstractToken> chunkToks = Tokenizer.tokenize(s);
        if (chunkToks != null) {
          allChunks.add(chunkToks);
          parseableLineNos.add(lineno);
        } else {
          unparseableStrs.add(s);
          unparseableLineNos.add(lineno);
        }
        s = in.readLine();
        lineno++;
      }
    } finally {
      in.close();
    }

    //
    // Infer type structure from the tokenized chunks
    //
    long start = System.currentTimeMillis();
    System.err.println("Number of chunks: " + allChunks.size());
    InferredType typeTree = TypeInference.infer(allChunks);
    long end = System.currentTimeMillis();
    double loadTime = (start - startRead) / 1000.0;
    double inferTime = (end - start) / 1000.0;
    double totalTime = (end - startRead) / 1000.0;
    System.err.println("Elapsed load time: " + loadTime);
    System.err.println("Elapsed inference time: " + inferTime);
    System.err.println("Total execution time: " + totalTime);

    //
    // The existing type tree is now correct, but could probably be more succinct.
    // We can now improve/rewrite it.
    //

    //
    // Should every top-level type be ARRAY, so as to allow repeated log lines?
    // Or does the Avro format allow an implict top-level repeating structure?
    //

    //
    // Dump the results.  We emit:
    // 1) A JSON/Avro schema
    // 2) A serialized parser program that can consume data and emit Avro files using the given schema
    //
    Schema s = typeTree.getAvroSchema();
    BufferedWriter out = new BufferedWriter(new FileWriter(schemaFile));
    try {
      out.write(s.toString(true));
    } finally {
      out.close();
    }
    DataOutputStream outd = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(parseTreeFile)));
    try {
      typeTree.write(outd);
    } finally {
      outd.close();
    }

    //
    // Apply the typetree's parser.
    //
    if (emitAvro) {
      int numGoodParses = 0;
      int lineno = 0;
      Schema schema = typeTree.getAvroSchema();
      GenericDatumWriter jsonGDWriter = new GenericDatumWriter(schema);
      BufferedOutputStream outJson = new BufferedOutputStream(new FileOutputStream(jsonDataFile));
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, outJson);

      GenericDatumWriter gdWriter = new GenericDatumWriter(schema);
      DataFileWriter outData = new DataFileWriter(gdWriter);
      outData = outData.create(schema, dataFile);

      try {
        in = new BufferedReader(new FileReader(f));
        try {
          //System.err.println("Type tree root is " + typeTree);
          String str = in.readLine();
          while (str != null) {
            GenericContainer gct = typeTree.parse(str);

            if (gct != null) {
              numGoodParses++;
              jsonGDWriter.write(gct, encoder);
              outData.append(gct);
              //System.err.println("Good parse " + numGoodParses);
            } else {
              System.err.println("unparsed line: '" + str + "'");
            }
            str = in.readLine();
            lineno++;
          }      
        } finally {
          in.close();
        }
      } finally {
        encoder.flush();
        outJson.close();
        outData.close();
      }
      System.err.println();
      System.err.println("Total # input lines: " + lineno);
      System.err.println("Total # lines parsed correctly: " + numGoodParses);
    }
  }

  //////////////////////////////////////////
  // main()
  //////////////////////////////////////////
  public static void main(String argv[]) throws IOException {
    if (argv.length < 2) {
      System.err.println("Usage: LearnStructure <input-datafile> <outdir> (-emitAvro (true)|false)");
      return;
    }
    boolean emitAvro = true;
    int i = 0;
    File f = new File(argv[i++]).getCanonicalFile();
    File outdir = new File(argv[i++]).getCanonicalFile();
    for (; i < argv.length; i++) {
      if ("-emitAvro".equals(argv[i])) {
        i++;
        emitAvro = "true".equals(argv[i]);
      }
    }

    System.err.println("Input file: " + f.getCanonicalPath());
    System.err.println("Output directory: " + outdir.getCanonicalPath());
    if (outdir.exists()) {
      throw new IOException("Output directory already exists: " + outdir);
    }
    outdir.mkdirs();

    LearnStructure ls = new LearnStructure();
    ls.inferRecordFormat(f, outdir, emitAvro);
  }
}
