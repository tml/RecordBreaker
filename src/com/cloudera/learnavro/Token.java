// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.io.*;
import java.util.*;

/*********************************************************
 * Token is one of a handful of data types we expect to appear broadly in 
 * log-style data: ints, floats, dates, IP addrs, timestamps, etc, etc.
 *
 * This file contains the superclass Token as well as the many subclasses.
 *********************************************************/
public class Token {
  static int EPOCH_START_YEAR = 1970;

  static class AbstractToken {
    public String getId() {
      return null;
    }
  }

  static class MetaToken extends AbstractToken {
    public static String tokenTypeLabel = "meta";
    CharToken start;
    CharToken end;
    List<AbstractToken> contents;

    /**
     */
    public MetaToken(CharToken start, CharToken end, List<AbstractToken> contents) {
      this.start = start;
      this.end = end;
      this.contents = contents;
    }
    public CharToken getStartToken() {
      return start;
    }
    public CharToken getEndToken() {
      return end;
    }
    public List<Token.AbstractToken> getMiddleChunk() {
      return contents;
    }
    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("META(" + start + "..." + end + "\n");
      for (AbstractToken tok: contents) {
        buf.append("\t" + tok + "\n");
      }
      buf.append(")");
      return buf.toString();
    }
    public String getId() {
      return "meta:" + start.getChar();
    }
  }

  static class CharToken extends AbstractToken {
    public static String tokenTypeLabel = "char";
    char c;
    public CharToken(char c) {
      this.c = c;
    }
    public char getChar() {
      return c;
    }
    public String toString() {
      return "CHAR(" + c + ")";
    }
    public String getId() {
      return "char:" + c;
    }
  }

  static class IPAddrToken extends AbstractToken {
    public static String tokenTypeLabel = "ipaddr";
    String s;
    public IPAddrToken(String s) {
      this.s = s;
    }
    public String toString() {
      return "IPADDR(" + s + ")";
    }
    public String getId() {
      return "ipaddr";
    }
  }

  static class PermissionBits extends AbstractToken {
    public static String tokenTypeLabel = "permissionbits";
    String s;
    public PermissionBits(String s) {
      this.s = s;
    }
    public String toString() {
      return "PERMISSION-BITS(" + s + ")";
    }
    public String getId() {
      return "permissionbits";
    }
  }

  static class DateToken extends AbstractToken {
    public static String tokenTypeLabel = "date";
    String month;
    int day;
    int year;
    public DateToken(String dayStr, String monthStr) throws IOException {
      try {
        this.day = Integer.parseInt(dayStr);
        if (day < 1 || day > 31) {
          throw new IOException("Illegal day value: " + day);
        }
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
      this.month = monthStr;
      this.year = -1;
    }
    public DateToken(String dayStr, String monthStr, String yrStr) throws IOException {
      try {
        this.day = Integer.parseInt(dayStr);
        if (day < 1 || day > 31) {
          throw new IOException("Illegal day value: " + day);
        }
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
      this.month = monthStr;
      try {
        this.year = Integer.parseInt(yrStr);
        if (year < EPOCH_START_YEAR) {
          throw new IOException("Illegal year value: " + year);
        }
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
    }
    public String toString() {
      return "DATE(" + day + ", " + month + ", " + year + ")";
    }
    public String getId() {
      return "date";
    }
  }

  static class TimeToken extends AbstractToken {
    public static String tokenTypeLabel = "time";
    int hr;
    int min;
    int sec;
    public TimeToken(String hrS, String minS, String secS) {
      try {
        this.hr = Integer.parseInt(hrS);
        this.min = Integer.parseInt(minS);
        this.sec = Integer.parseInt(secS);
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
    }
    public String toString() {
      return "TIME(" + hr + ":" + min + ":" + sec + ")";
    }
    public String getId() {
      return "time";
    }
  }

  static class IntToken extends AbstractToken {
    public static String tokenTypeLabel = "int";
    int i;
    public IntToken(String s) {
      try {
        this.i = Integer.parseInt(s);
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
    }
    public String toString() {
      return "INT(" + i + ")";
    }
    public String getId() {
      return "int";
    }
  }

  static class FloatToken extends AbstractToken {
    public static String tokenTypeLabel = "float";
    double f;
    public FloatToken(String s) {
      try {
        this.f = Double.parseDouble(s);
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
    }
    public String toString() {
      return "FLOAT(" + f + ")";
    }
    public String getId() {
      return "float";
    }
  }

  static class StringToken extends AbstractToken {
    public static String tokenTypeLabel = "string";
    String s;
    public StringToken(String s) {
      this.s = s;
    }
    public String toString() {
      return "STRING(" + s + ")";
    }
    public String getId() {
      return "str";
    }
  }

  static class EOLToken extends AbstractToken {
    public static String tokenTypeLabel = "eol";
    public EOLToken() {
    }
    public String toString() {
      return "EOL()";
    }
    public String getId() {
      return "eol";
    }
  }

  static class WhitespaceToken extends AbstractToken {
    public static String tokenTypeLabel = "ws";
    public WhitespaceToken() {
    }
    public String toString() {
      return "WS()";
    }
    public String getId() {
      return "whitespace";
    }
  }

  static class NoopToken extends AbstractToken {
    public static String tokenTypeLabel = "noop";
    public NoopToken() {
    }
    public String toString() {
      return "NOOP()";
    }
    public String getId() {
      return "noop";
    }
  }
}

