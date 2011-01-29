// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.io.*;
import java.util.*;
import org.apache.avro.Schema;

/*********************************************************
 * Token is one of a handful of data types we expect to appear broadly in 
 * log-style data: ints, floats, dates, IP addrs, timestamps, etc, etc.
 *
 * This file contains the superclass Token as well as the many subclasses.
 *********************************************************/
public class Token {
  static int EPOCH_START_YEAR = 1970;

  final static int META_TOKENCLASSID = 0;
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
  
  static class AbstractToken {
    public static boolean hasData(int tokenClassIdentifier) {
    switch (tokenClassIdentifier) {
    case META_TOKENCLASSID:
      return false;
    case CHAR_TOKENCLASSID:
      return false;
    case IPADDR_TOKENCLASSID:
      return true;
    case PERMISSIONS_TOKENCLASSID:
      return true;
    case DATE_TOKENCLASSID:
      return true;
    case TIME_TOKENCLASSID:
      return true;
    case INT_TOKENCLASSID:
      return true;
    case FLOAT_TOKENCLASSID:
      return true;
    case STRING_TOKENCLASSID:
      return true;
    case EOL_TOKENCLASSID:
      return false;
    case WHITESPACE_TOKENCLASSID:
      return false;
    case NOOP_TOKENCLASSID:
      return false;
    default:
      // should never happen
      return false;
    }
    }
    public static String getClassStr(int tokenClassIdentifier) {
    switch (tokenClassIdentifier) {
    case META_TOKENCLASSID:
      return "meta";
    case CHAR_TOKENCLASSID:
      return "char";
    case IPADDR_TOKENCLASSID:
      return "ipaddr";
    case PERMISSIONS_TOKENCLASSID:
      return "permissionbits";
    case DATE_TOKENCLASSID:
      return "date";
    case TIME_TOKENCLASSID:
      return "time";
    case INT_TOKENCLASSID:
      return "int";
    case FLOAT_TOKENCLASSID:
      return "float";
    case STRING_TOKENCLASSID:
      return "string";
    case EOL_TOKENCLASSID:
      return "eol";
    case WHITESPACE_TOKENCLASSID:
      return "ws";
    case NOOP_TOKENCLASSID:
      return "noop";
    default:
      // should never happen
      return null;
    }
    }
    public static Schema createAvroSchema(int tokenClassIdentifier, String tokenParameter) {
    switch (tokenClassIdentifier) {
    case META_TOKENCLASSID:
      return null;
    case CHAR_TOKENCLASSID:
      return null;
    case IPADDR_TOKENCLASSID:
      return Schema.create(Schema.Type.STRING);
    case PERMISSIONS_TOKENCLASSID:
      return Schema.create(Schema.Type.STRING);
    case DATE_TOKENCLASSID: {
      List<Schema.Field> fields = new ArrayList<Schema.Field>();
      fields.add(new Schema.Field("month", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("day", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("year", Schema.create(Schema.Type.INT), "", null));
      return Schema.createRecord(fields);
    }
    case TIME_TOKENCLASSID: {
      List<Schema.Field> fields = new ArrayList<Schema.Field>();
      fields.add(new Schema.Field("hrs", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("mins", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("secs", Schema.create(Schema.Type.INT), "", null));
      return Schema.createRecord(fields);
    }
    case INT_TOKENCLASSID:
      return Schema.create(Schema.Type.INT);
    case FLOAT_TOKENCLASSID:
      return Schema.create(Schema.Type.DOUBLE);
    case STRING_TOKENCLASSID:
      return Schema.create(Schema.Type.STRING);
    case EOL_TOKENCLASSID:
      return null;
    case WHITESPACE_TOKENCLASSID:
      return null;
    case NOOP_TOKENCLASSID:
      return null;
    default:
      // should never happen
      return null;
    }
    }

    public static String getStrDesc(int classId, String tokenParameter) {
      return getClassStr(classId) + ((tokenParameter != null) ? tokenParameter : "");
    }

    int classId;
    String tokenParameter;
    public AbstractToken(int classId, String tokenParameter) {
      this.classId = classId;
      this.tokenParameter = tokenParameter;
    }
    public int getClassId() {
      return classId;
    }
    public String getParameter() {
      return tokenParameter;
    }
    public String getId() {
      return getStrDesc(classId, tokenParameter);
    }
  }

  static class MetaToken extends AbstractToken {
    CharToken start;
    CharToken end;
    List<AbstractToken> contents;

    /**
     */
    public MetaToken(CharToken start, CharToken end, List<AbstractToken> contents) {
      super(META_TOKENCLASSID, null);
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
    public String getParameter() {
      return "" + start.getChar();
    }
  }

  static class CharToken extends AbstractToken {
    char c;
    public CharToken(char c) {
      super(CHAR_TOKENCLASSID, "" + c);
      this.c = c;
    }
    public char getChar() {
      return c;
    }
    public String toString() {
      return "CHAR(" + c + ")";
    }
  }

  static class IPAddrToken extends AbstractToken {
    String s;
    public IPAddrToken(String s) {
      super(IPADDR_TOKENCLASSID, null);
      this.s = s;
    }
    public String toString() {
      return "IPADDR(" + s + ")";
    }
  }

  static class PermissionBits extends AbstractToken {
    String s;
    public PermissionBits(String s) {
      super(PERMISSIONS_TOKENCLASSID, null);
      this.s = s;
    }
    public String toString() {
      return "PERMISSION-BITS(" + s + ")";
    }
  }

  static class DateToken extends AbstractToken {
    String month;
    int day;
    int year;
    public DateToken(String dayStr, String monthStr) throws IOException {
      super(DATE_TOKENCLASSID, null);
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
      super(DATE_TOKENCLASSID, null);
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
  }

  static class TimeToken extends AbstractToken {
    int hr;
    int min;
    int sec;
    public TimeToken(String hrS, String minS, String secS) {
      super(TIME_TOKENCLASSID, null);
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
  }

  static class IntToken extends AbstractToken {
    int i;
    public IntToken(String s) {
      super(INT_TOKENCLASSID, null);
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

  static class FloatToken extends AbstractToken {
    double f;
    public FloatToken(String s) {
      super(FLOAT_TOKENCLASSID, null);
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

  static class StringToken extends AbstractToken {
    String s;
    public StringToken(String s) {
      super(STRING_TOKENCLASSID, null);
      this.s = s;
    }
    public String toString() {
      return "STRING(" + s + ")";
    }
  }

  static class EOLToken extends AbstractToken {
    public EOLToken() {
      super(EOL_TOKENCLASSID, null);
    }
    public String toString() {
      return "EOL()";
    }
  }

  static class WhitespaceToken extends AbstractToken {
    public WhitespaceToken() {
      super(WHITESPACE_TOKENCLASSID, null);
    }
    public String toString() {
      return "WS()";
    }
  }

  static class NoopToken extends AbstractToken {
    public NoopToken() {
      super(NOOP_TOKENCLASSID, null);
    }
    public String toString() {
      return "NOOP()";
    }
  }
}

