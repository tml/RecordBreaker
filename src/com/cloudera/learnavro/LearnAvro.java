// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.io.*;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;

/*********************************************************
 * LearnAvro is the main file for figuring out pattern-extractors and schemas for a text file.
 *
 * This code operates on a raw text file and emits the extractors/schemas.  The user
 * may decide to remove certain extractors/schemas if they only apply to a tiny number of
 * potential lines in the target text file.
 *
 *********************************************************/
public class LearnAvro {
  static String SCHEMA_FILENAME = "schema.json";
  static String DATA_FILENAME = "data.avro";
  static String PARSER_FILENAME = "parser.dat";

  /**
   *
   */
  public RecordFormat inferRecordFormat(File f, File outdir, boolean emitAvro) throws IOException {
    File schemaFile = new File(outdir, SCHEMA_FILENAME);
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
    InferredType typeTree = TypeInference.infer(allChunks);
    long end = System.currentTimeMillis();
    double loadTime = (start - startRead) / 1000.0;
    double inferTime = (end - start) / 1000.0;
    double totalTime = (end - startRead) / 1000.0;
    System.err.println("Total elapsed time: " + totalTime);
    System.err.println("Elapsed load time: " + loadTime);
    System.err.println("Elapsed inference time: " + inferTime);
    System.err.println("Ratio load-to-inference: " + (loadTime / inferTime));

    //
    // The existing type tree is now correct, but could probably be more succinct.
    // We can now improve/rewrite it.
    //

    //
    // Should every top-level type be ARRAY, so as to allow repeated log lines?
    // Or does the Avro format allow an implict top-level repeating structure?
    //

    //
    // Apply the typetree's parser.
    //
    if (emitAvro) {
      Schema schema = typeTree.getAvroSchema();
      GenericDatumWriter gdWriter = new GenericDatumWriter(schema);
      BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dataFile));
      JsonEncoder encoder = new JsonEncoder(schema, out);
      try {
        in = new BufferedReader(new FileReader(f));
        try {
          String s = in.readLine();
          int lineno = 0;
          while (s != null) {
            GenericContainer gct = typeTree.parse(s);
            gdWriter.write(gct, encoder);
            System.err.println("Data for line " + lineno + ": " + gct);
            s = in.readLine();
            lineno++;
          }      
        } finally {
          in.close();
        }
      } finally {
        encoder.flush();
        out.close();
      }
    }

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
    
    return null;
  }

  //////////////////////////////////////////
  // main()
  //////////////////////////////////////////
  public static void main(String argv[]) throws IOException {
    if (argv.length < 2) {
      System.err.println("Usage: LearnAvro <input-datafile> <outdir> (-emitAvro true|false)");
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

    LearnAvro la = new LearnAvro();
    RecordFormat rf = la.inferRecordFormat(f, outdir, emitAvro);
    System.err.println("RecordFormat: " + rf);
  }
}
