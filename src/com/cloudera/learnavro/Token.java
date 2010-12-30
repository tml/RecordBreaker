// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.util.*;

/*********************************************************
 * Token is one of a handful of data types we expect to appear broadly in 
 * log-style data: ints, floats, dates, IP addrs, timestamps, etc, etc.
 *
 * This file contains the superclass Token as well as the many subclasses.
 *********************************************************/
public class Token {
}

class MetaToken extends Token {
  CharToken start;
  CharToken end;
  List<Token> contents;

  /**
   */
  public MetaToken(CharToken start, CharToken end, List<Token> contents) {
    this.start = start;
    this.end = end;
    this.contents = contents;
  }
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("META(" + start + "..." + end + "\n");
    for (Token tok: contents) {
      buf.append("\t" + tok + "\n");
    }
    buf.append(")");
    return buf.toString();
  }
}

class CharToken extends Token {
  char c;
  public CharToken(char c) {
    this.c = c;
  }
  public String toString() {
    return "CHAR(" + c + ")";
  }
}

class IPAddrToken extends Token {
  String s;
  public IPAddrToken(String s) {
    this.s = s;
  }
  public String toString() {
    return "IPADDR(" + s + ")";
  }
}

class TimeToken extends Token {
  int hr;
  int min;
  int sec;
  public TimeToken(String hrS, String minS, String secS) {
    try {
      this.hr = Integer.parseInt(hrS);
      this.min = Integer.parseInt(minS);
      this.sec = Integer.parseInt(secS);
    } catch (NumberFormatException nfe) {
    }
  }
  public String toString() {
    return "TIME(" + hr + ":" + min + ":" + sec + ")";
  }
}

class IntToken extends Token {
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
}

class FloatToken extends Token {
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
}

class StringToken extends Token {
  String s;
  public StringToken(String s) {
    this.s = s;
  }
  public String toString() {
    return "STRING(" + s + ")";
  }
}

class EOLToken extends Token {
  public EOLToken() {
  }
  public String toString() {
    return "EOL()";
  }
}

class WhitespaceToken extends Token {
  public WhitespaceToken() {
  }
  public String toString() {
    return "WS()";
  }
}

class NoopToken extends Token {
  public NoopToken() {
  }
  public String toString() {
    return "NOOP()";
  }
}
