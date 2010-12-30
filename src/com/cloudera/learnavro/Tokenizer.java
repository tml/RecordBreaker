// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.io.*;
import java.util.*;
import java.util.regex.*;

/*********************************************************
 * Tokenizer transforms a line of text into a set of Token objects.
 * Each Token is one of a handful of classes.
 *
 *********************************************************/
public class Tokenizer {
  static Pattern ipAddrPattern = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+(?:\\.\\d+)?)");
  static Pattern timePattern1 = Pattern.compile("(\\d\\d):(\\d\\d):(\\d\\d)");
  static Pattern timePattern2 = Pattern.compile("(\\d\\d):(\\d\\d)");
  static Pattern intPattern = Pattern.compile("([-+]?\\d+)");
  static Pattern intRangePattern = Pattern.compile("(\\d+)-(\\d+)");
  static Pattern floatPattern = Pattern.compile("([+-]?\\d*\\.\\d+)");
  static Pattern floatRangePattern = Pattern.compile("(\\d*\\.\\d+)-(\\d*\\.\\d+)");
  static Pattern stringPattern = Pattern.compile("(\\S{2,})");
  static Pattern charPattern = Pattern.compile("(\\S)");
  static Pattern eolPattern = Pattern.compile("(\\n)");
  static Pattern wsPattern = Pattern.compile("(\\s+)");
  static HashMap<String, String> complements;
  static HashMap<String, String> reverseComplements;

  static {
    complements = new HashMap<String, String>();
    complements.put("[", "]");
    complements.put("{", "}");
    complements.put("\"", "\"");
    complements.put("'", "'");
    complements.put("<", ">");
    complements.put("(", ")");
    reverseComplements = new HashMap<String, String>();
    reverseComplements.put("]", "[");
    reverseComplements.put("}", "{");
    reverseComplements.put("\"", "\"");
    reverseComplements.put("'", "'");
    reverseComplements.put(">", "<");
    reverseComplements.put(")", "(");
  }

  private static String cutChunk(Matcher m, String curS) {
    int lastGroupChar = m.end(m.groupCount());
    if (curS.length() > lastGroupChar) {
      return curS.substring(lastGroupChar);
    } else {
      return "";
    }
  }

  /**
   * Accepts a single line of input, returns all the tokens for that line.
   * If the line cannot be parsed, we return null.
   */
  static public List<Token> tokenize(String s) throws IOException {
    String curS = s;
    List<Token> toksSoFar = new ArrayList<Token>();

    // We now repeatedly pass through a series of text-extractor tests.
    //System.err.println("PARSE: " + s);
    while (curS.length() > 0) {
      //System.err.println("CurS: '" + curS + "', tokSetSize: " + toksSoFar.size());
      int newStart = -1;

      // 1.  META
      char startChar = curS.charAt(0);
      if (complements.get("" + startChar) != null) {
        //System.err.println("START CHAR: " + startChar);
        String closeChar = complements.get("" + startChar);
        int closeIndex = curS.indexOf(closeChar, 1);
        if (closeIndex >= 0) {
          toksSoFar.add(new MetaToken(new CharToken(curS.charAt(0)), new CharToken(closeChar.charAt(0)), tokenize(curS.substring(1, closeIndex))));
          curS = curS.substring(closeIndex+1);
          continue;
        }
      }

      // 2.  IP ADDR
      Matcher m = ipAddrPattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new IPAddrToken(m.group(1)));
        curS = cutChunk(m, curS);
        continue;
      }

      // 3.  TIME
      m = timePattern1.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new TimeToken(m.group(1), m.group(2), m.group(3)));
        curS = cutChunk(m, curS);
        continue;
      }
      m = timePattern2.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new TimeToken(m.group(1), m.group(2), "00"));
        curS = cutChunk(m, curS);
        continue;
      }

      // 4. FLOAT RANGE
      m = floatRangePattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new FloatToken(m.group(1)));
        toksSoFar.add(new CharToken('-'));
        toksSoFar.add(new FloatToken(m.group(2)));
        curS = cutChunk(m, curS);
        continue;
      }

      // 5. INTEGER RANGE
      // REMIND - mjc - Should there be a dedicated Token class for ranges?
      m = intRangePattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new IntToken(m.group(1)));
        toksSoFar.add(new CharToken('-'));
        toksSoFar.add(new IntToken(m.group(2)));
        curS = cutChunk(m, curS);
        continue;
      }

      // 6. FLOAT
      m = floatPattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new FloatToken(m.group(1)));
        curS = cutChunk(m, curS);
        continue;
      }

      // 7. INTEGER
      m = intPattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new IntToken(m.group(1)));
        curS = cutChunk(m, curS);
        continue;
      }

      // 8. STRING
      m = stringPattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new StringToken(m.group(1)));
        curS = cutChunk(m, curS);
        continue;
      }

      // 9. CHAR
      m = charPattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new CharToken(m.group(1).charAt(0)));
        curS = cutChunk(m, curS);
        continue;
      }

      // EOL-Token
      m = eolPattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new EOLToken());
        curS = cutChunk(m, curS);
        continue;
      }

      // Whitespace
      m = wsPattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new WhitespaceToken());
        curS = cutChunk(m, curS);
        continue;
      }

      // DEFAULT
      // If execution reaches this point, it means no pattern applied, which means the line cannot be parsed.
      return null;
    }
    return toksSoFar;
  }

  ///////////////////////////////////////////////////
  // main() tests the Tokenizer.
  ////////////////////////////////////////////////////
  public static void main(String argv[]) throws IOException {
    if (argv.length < 1) {
      System.err.println("Usage: Tokenizer <datafile> (-verbose)");
      return;
    }
    File f = new File(argv[0]).getCanonicalFile();
    boolean verbose = false;
    for (int i = 1; i < argv.length; i++) {
      if ("-verbose".equals(argv[i])) {
        verbose = true;
      }
    }
    System.err.println("Input file: " + f.getCanonicalPath());

    // Store parse errors and results
    List<Integer> unparseableLineNos = new ArrayList<Integer>();
    List<String> unparseableStrs = new ArrayList<String>();
    List<Integer> parseableLineNos = new ArrayList<Integer>();
    List<List<Token>> allChunks = new ArrayList<List<Token>>();

    // Transform the text into a list of "chunks".  
    // A single chunk corresponds to a line of text.  A chunk is a list of Tokens.
    int totalCount = 0;
    int parsedCount = 0;
    int errorCount = 0;
    BufferedReader in = new BufferedReader(new FileReader(f));
    try {
      String s = in.readLine();
      int lineno = 0;
      while (s != null) {
        List<Token> chunkToks = Tokenizer.tokenize(s);
        if (chunkToks != null) {
          allChunks.add(chunkToks);
          parseableLineNos.add(lineno);
          parsedCount++;
        } else {
          unparseableStrs.add(s);
          unparseableLineNos.add(lineno);
          errorCount++;
        }
        s = in.readLine();
        lineno++;
        totalCount++;
      }
    } finally {
      in.close();
    }

    System.err.println();
    System.err.println("Total lines: " + totalCount);
    System.err.println("Parsed lines: " + parsedCount + " (" + (1.0*parsedCount / totalCount) + ")");
    System.err.println("Error lines: " + errorCount + " (" + (1.0*errorCount / totalCount) + ")");

    //
    // Print out parsed tokens
    //
    if (verbose) {
      System.err.println();
      System.err.println("--RESULTS--------");
      int i = 0;
      for (List<Token> chunk: allChunks) {
        System.err.print(parseableLineNos.get(i) + ".  ");
        for (Token tok: chunk) {
          System.err.print(tok + "  ");
        }
        System.err.println();
        i++;
      }

      //
      // Print out error strings
      //
      System.err.println();
      System.err.println("--ERRORS---------");
      i = 0;
      for (String s: unparseableStrs) {
        System.err.println(unparseableLineNos.get(i) + ".  " + s);
        i++;
      }
    }
  }
}
