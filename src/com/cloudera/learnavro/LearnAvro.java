// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.io.*;
import java.util.*;

/*********************************************************
 * LearnAvro is the main file for figuring out pattern-extractors and schemas for a text file.
 *
 * This code operates on a raw text file and emits the extractors/schemas.  The user
 * may decide to remove certain extractors/schemas if they only apply to a tiny number of
 * potential lines in the target text file.
 *
 *********************************************************/
public class LearnAvro {

  /**
   *
   */
  public RecordFormat inferRecordFormat(File f) throws IOException {
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
    return null;
  }

  //////////////////////////////////////////
  // main()
  //////////////////////////////////////////
  public static void main(String argv[]) throws IOException {
    if (argv.length < 1) {
      System.err.println("Usage: LearnAvro <datafile>");
      return;
    }
    File f = new File(argv[0]).getCanonicalFile();
    System.err.println("Input file: " + f.getCanonicalPath());
    LearnAvro la = new LearnAvro();
    RecordFormat rf = la.inferRecordFormat(f);
    System.err.println("RecordFormat: " + rf);
  }
}
