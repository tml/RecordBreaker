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
    List<List<Token>> allChunks = new ArrayList<List<Token>>();

    // Transform the text into a list of "chunks".  
    // A single chunk corresponds to a line of text.  A chunk is a list of Tokens.
    BufferedReader in = new BufferedReader(new FileReader(f));
    try {
      String s = in.readLine();
      int lineno = 0;
      while (s != null) {
        List<Token> chunkToks = Tokenizer.tokenize(s);
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

    // Extract the formats that characterize this set of 
    //format.assignNames();
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
