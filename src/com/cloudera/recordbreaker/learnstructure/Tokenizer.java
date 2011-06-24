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
import java.util.regex.*;

/*********************************************************
 * Tokenizer transforms a line of text into a set of Token objects.
 * Each Token is one of a handful of classes.
 *
 *********************************************************/
public class Tokenizer {
  // The components of possible date patterns
  static String monthPatternStrs[] = {"(January|Jan|jan|February|Feb|feb|March|Mar|mar|April|Apr|apr|May|may|June|Jun|jun|July|Jul|jul|August|Aug|aug|September|Sep|sep|October|Oct|oct|November|Nov|nov|December|Dec|dec)", "([01]*\\d)"};
  static String dateSeparatorPatternStrs[] = {"(?:\\s+)", "(?:\\.)", "(?:\\/)"};
  static String dateDayPatternStr = "([0123]?\\d)";
  static String dateYearPatternStr = "([12]\\d{3})";

  static List<Pattern> monthFirstPatterns = new ArrayList<Pattern>();
  static List<Pattern> yearFirstPatterns = new ArrayList<Pattern>();
  static List<Pattern> dayFirstPatterns = new ArrayList<Pattern>();

  static Pattern ipAddrPattern = Pattern.compile("((?:(?:\\d+\\.){3,}\\d+)|(?:\\*\\.(?:(?:\\d+|\\*)\\.)*(?:\\d+|\\*)))");
  static Pattern permissionBitPattern = Pattern.compile("([drwx-]{9,})");
  static Pattern timePattern1 = Pattern.compile("(\\d\\d):(\\d\\d):(\\d\\d)");
  static Pattern timePattern2 = Pattern.compile("(\\d\\d):(\\d\\d)");
  static Pattern intPattern = Pattern.compile("([-+]?\\d+)");
  static Pattern intRangePattern = Pattern.compile("(\\d+)-(\\d+)");
  static Pattern floatPattern = Pattern.compile("([+-]?\\d*\\.\\d+)");
  static Pattern floatRangePattern = Pattern.compile("(\\d*\\.\\d+)-(\\d*\\.\\d+)");
  static Pattern stringPattern = Pattern.compile("((?:[\\S&&[^\\\"\\,\\;\\|\\[\\]\\{\\}\\<\\>\\(\\)\\']]){2,})");
  static Pattern charPattern = Pattern.compile("(\\S)");
  static Pattern eolPattern = Pattern.compile("(\\n)");
  static Pattern wsPattern = Pattern.compile("(\\s+)");
  static HashMap<String, String> complements;
  static HashMap<String, String> reverseComplements;

  /**
   * attemptParse() tries to parse the input string with the given token-class.  
   * If successful, it returns the remaining string and adds the token to the given list.
   * If not successful, it returns null and does not modify the given list.
   *
   * This method is used in two places:
   * 1) text-tokenization during the structure-learning phase
   * 2) Guided parsing, after a learned structure-parser has been constructed.
   *
   * For case #1, we expect that a loop will call attemptParse repeatedly, until it find a token-type
   * that can be correctly parsed.  This is what happens inside tokenize() below.
   *
   * For case #2, we expect that the parse-tree will contain a specific token-type that *must* be
   * parsed, or else that branch of the parse-tree is invalid.  This is what happens inside
   * InferredType.BaseType.internalParse().
   */
  public static String attemptParse(int tokenClassId, String tokenParameter, String inputStr, List<Token.AbstractToken> outputToks) {
    switch (tokenClassId) {
    case Token.IPADDR_TOKENCLASSID: {
      Matcher m = ipAddrPattern.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.IPAddrToken(m.group(1)));
        return cutChunk(m, inputStr);
      } else {
        return null;
      }
    }
    case Token.PERMISSIONS_TOKENCLASSID: {
      Matcher m = permissionBitPattern.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.PermissionBits(m.group(1)));
        return cutChunk(m, inputStr);
      } else {
        return null;
      }
    }
    case Token.DATE_TOKENCLASSID: {
      String newStr = null;
      for (Pattern p: monthFirstPatterns) {
        Matcher m = p.matcher(inputStr);
        if (m.lookingAt()) {
          if (m.groupCount() == 2) {
            try {
              outputToks.add(new Token.DateToken(m.group(2), m.group(1)));
            } catch (IOException iex) {
              continue;
            }
          } else {
            try {
              outputToks.add(new Token.DateToken(m.group(2), m.group(1), m.group(3)));
            } catch (IOException iex) {
              continue;
            }
          }
          return cutChunk(m, inputStr);
        }
      }

      for (Pattern p: yearFirstPatterns) {
        Matcher m = p.matcher(inputStr);
        if (m.lookingAt()) {
          try {
            outputToks.add(new Token.DateToken(m.group(3), m.group(2), m.group(1)));
          } catch (IOException iex) {
            continue;
          }
          return cutChunk(m, inputStr);
        }
      }

      for (Pattern p: dayFirstPatterns) {
        Matcher m = p.matcher(inputStr);
        if (m.lookingAt()) {
          if (m.groupCount() == 2) {
            try {
              outputToks.add(new Token.DateToken(m.group(1), m.group(2)));
            } catch (IOException iex) {
              continue;
            }
          } else {
            try {
              outputToks.add(new Token.DateToken(m.group(1), m.group(2), m.group(3)));
            } catch (IOException iex) {
              continue;
            }
          }
          return cutChunk(m, inputStr);
        }
      }
      return null;
    }
    case Token.TIME_TOKENCLASSID: {
      Matcher m = timePattern1.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.TimeToken(m.group(1), m.group(2), m.group(3)));
        return cutChunk(m, inputStr);
      }
      m = timePattern2.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.TimeToken(m.group(1), m.group(2), "00"));
        return cutChunk(m, inputStr);
      }
      return null;
    }
    case Token.CHAR_TOKENCLASSID: {
      Matcher m = charPattern.matcher(inputStr);
      if (m.lookingAt()) {
        if (tokenParameter != null && ! tokenParameter.equals("" + m.group(1).charAt(0))) {
          return null;
        }
        outputToks.add(new Token.CharToken(m.group(1).charAt(0)));
        return cutChunk(m, inputStr);
      }
      return null;
    }
    case Token.FLOAT_TOKENCLASSID: {
      Matcher m = floatPattern.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.FloatToken(m.group(1)));
        return cutChunk(m, inputStr);
      }
      return null;
    }
    case Token.INT_TOKENCLASSID: {
      Matcher m = intPattern.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.IntToken(m.group(1)));
        return cutChunk(m, inputStr);
      }
      return null;
    }
    case Token.STRING_TOKENCLASSID: {
      Matcher m = stringPattern.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.StringToken(m.group(1)));
        return cutChunk(m, inputStr);
      }
      return null;
    }
      // CHAR???
    case Token.EOL_TOKENCLASSID: {
      Matcher m = eolPattern.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.EOLToken());
        return cutChunk(m, inputStr);
      }
      return null;
    }
    case Token.WHITESPACE_TOKENCLASSID: {
      Matcher m = wsPattern.matcher(inputStr);
      if (m.lookingAt()) {
        outputToks.add(new Token.WhitespaceToken());
        return cutChunk(m, inputStr);
      }
      return null;
    }
    default: {
      return null;
    }
    }
  }

  private static String cutChunk(Matcher m, String curS) {
    int lastGroupChar = m.end(m.groupCount());
    if (curS.length() > lastGroupChar) {
      return curS.substring(lastGroupChar);
    } else {
      return "";
    }
  }

  final static int CHAR_TOKENCLASSID = 1;
  final static int IPADDR_TOKENCLASSID = 2;
  final static int PERMISSIONS_TOKENCLASSID = 3;
  final static int DATE_TOKENCLASSID = 4;
  final static int TIME_TOKENCLASSID = 5;
  final static int INT_TOKENCLASSID = 6;
  final static int FLOAT_TOKENCLASSID = 7;
  final static int STRING_TOKENCLASSID = 8;
  final static int EOL_TOKENCLASSID = 9;
  final static int WHITESPACE_TOKENCLASSID = 10;
  final static int NOOP_TOKENCLASSID = 11;

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

    // Construct the date patterns
    for (String separatorPatternStr: dateSeparatorPatternStrs) {
      for (String monthPatternStr: monthPatternStrs) {
        // Create all legal combos of month, day, year, and separator
        monthFirstPatterns.add(Pattern.compile(monthPatternStr + separatorPatternStr + dateDayPatternStr + separatorPatternStr + dateYearPatternStr));
        yearFirstPatterns.add(Pattern.compile(dateYearPatternStr + separatorPatternStr + monthPatternStr + separatorPatternStr + dateDayPatternStr));
        dayFirstPatterns.add(Pattern.compile(dateDayPatternStr + separatorPatternStr + monthPatternStr + separatorPatternStr + dateYearPatternStr));
      }
    }
    for (String separatorPatternStr: dateSeparatorPatternStrs) {
      monthFirstPatterns.add(Pattern.compile(monthPatternStrs[0] + separatorPatternStr + dateDayPatternStr));
      dayFirstPatterns.add(Pattern.compile(dateDayPatternStr + separatorPatternStr + monthPatternStrs[0]));
    }
  }


  /**
   * Accepts a single line of input, returns all the tokens for that line.
   * If the line cannot be parsed, we return null.
   */
  static public List<Token.AbstractToken> tokenize(String s) throws IOException {
    String curS = s;
    List<Token.AbstractToken> toksSoFar = new ArrayList<Token.AbstractToken>();

    // We now repeatedly pass through a series of text-extractor tests.
    while (curS.length() > 0) {
      int newStart = -1;

      // META
      char startChar = curS.charAt(0);
      if (complements.get("" + startChar) != null) {
        String closeChar = complements.get("" + startChar);
        int closeIndex = curS.indexOf(closeChar, 1);
        if (closeIndex >= 0) {
          toksSoFar.add(new Token.MetaToken(new Token.CharToken(curS.charAt(0)), new Token.CharToken(closeChar.charAt(0)), tokenize(curS.substring(1, closeIndex))));
          curS = curS.substring(closeIndex+1);
          continue;
        }
      }

      // IP ADDR
      // PERMISSION BITS
      String attemptStr = attemptParse(Token.IPADDR_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // PERMISSION BITS
      attemptStr = attemptParse(Token.PERMISSIONS_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      //
      // DATE
      //
      // Because of the huge number of possible date patterns, and our desire to not perform 
      // multi-token parsing, the date-processing here is a bit of a mess.
      //
      attemptStr = attemptParse(Token.DATE_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // TIME
      attemptStr = attemptParse(Token.TIME_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // FLOAT RANGE
      Matcher m = floatRangePattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new Token.FloatToken(m.group(1)));
        toksSoFar.add(new Token.CharToken('-'));
        toksSoFar.add(new Token.FloatToken(m.group(2)));
        curS = cutChunk(m, curS);
        continue;
      }

      // INTEGER RANGE
      // REMIND - mjc - Should there be a dedicated Token class for ranges?
      m = intRangePattern.matcher(curS);
      if (m.lookingAt()) {
        toksSoFar.add(new Token.IntToken(m.group(1)));
        toksSoFar.add(new Token.CharToken('-'));
        toksSoFar.add(new Token.IntToken(m.group(2)));
        curS = cutChunk(m, curS);
        continue;
      }

      // FLOAT
      attemptStr = attemptParse(Token.FLOAT_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // INTEGER
      attemptStr = attemptParse(Token.INT_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // STRING
      attemptStr = attemptParse(Token.STRING_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // CHAR
      attemptStr = attemptParse(Token.CHAR_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // EOL-Token
      attemptStr = attemptParse(Token.EOL_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
        continue;
      }

      // Whitespace
      attemptStr = attemptParse(Token.WHITESPACE_TOKENCLASSID, null, curS, toksSoFar);
      if (attemptStr != null) {
        curS = attemptStr;
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
    List<List<Token.AbstractToken>> allChunks = new ArrayList<List<Token.AbstractToken>>();

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
        List<Token.AbstractToken> chunkToks = Tokenizer.tokenize(s);
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
      for (List<Token.AbstractToken> chunk: allChunks) {
        System.err.print(parseableLineNos.get(i) + ".  ");
        for (Token.AbstractToken tok: chunk) {
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
