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
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericData;

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
  
  static abstract class AbstractToken {
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
    public static Schema createAvroSchema(int tokenClassIdentifier, String tokenParameter, String fieldName) {
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
      Schema s = Schema.createRecord(fieldName, "", "", false);
      List<Schema.Field> fields = new ArrayList<Schema.Field>();
      fields.add(new Schema.Field("month", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("day", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("year", Schema.create(Schema.Type.INT), "", null));
      s.setFields(fields);
      return s;
    }
    case TIME_TOKENCLASSID: {
      Schema s = Schema.createRecord(fieldName, "", "", false);
      List<Schema.Field> fields = new ArrayList<Schema.Field>();
      fields.add(new Schema.Field("hrs", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("mins", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("secs", Schema.create(Schema.Type.INT), "", null));
      s.setFields(fields);
      return s;
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
    public abstract Object get();
    public abstract String getSampleString();
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
    public String getSampleString() {
      return toString();
    }
    public String getParameter() {
      return "" + start.getChar();
    }
    public Object get() {
      ArrayList<Object> getResults = new ArrayList<Object>();
      for (AbstractToken tok: contents) {
        getResults.add(tok.get());
      }
      return getResults;
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
    public String getSampleString() {
      return get().toString();
    }
    public Object get() {
      return c;
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
    public String getSampleString() {
      return get().toString();
    }
    public Object get() {
      return new Utf8(s);
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
    public String getSampleString() {
      return get().toString();
    }
    public Object get() {
      return new Utf8(s);
    }
  }

  static class DateToken extends AbstractToken {
    int month;
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
      this.month = convertMonthStr(monthStr);
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
      this.month = convertMonthStr(monthStr);
      try {
        this.year = Integer.parseInt(yrStr);
        if (year < EPOCH_START_YEAR) {
          throw new IOException("Illegal year value: " + year);
        }
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
    }
    int convertMonthStr(String monthStr) {
      try {
        return Integer.parseInt(monthStr);
      } catch (NumberFormatException nfe) {
      }
      if ("jan".equalsIgnoreCase(monthStr)) {
        return 1;
      }
      if ("feb".equalsIgnoreCase(monthStr)) {
        return 2;
      }
      if ("mar".equalsIgnoreCase(monthStr)) {
        return 3;
      }
      if ("apr".equalsIgnoreCase(monthStr)) {
        return 4;
      }
      if ("may".equalsIgnoreCase(monthStr)) {
        return 5;
      }
      if ("jun".equalsIgnoreCase(monthStr)) {
        return 6;
      }
      if ("jul".equalsIgnoreCase(monthStr)) {
        return 7;
      }
      if ("aug".equalsIgnoreCase(monthStr)) {
        return 8;
      }
      if ("sep".equalsIgnoreCase(monthStr)) {
        return 9;
      }
      if ("oct".equalsIgnoreCase(monthStr)) {
        return 10;
      }
      if ("nov".equalsIgnoreCase(monthStr)) {
        return 11;
      }
      if ("dec".equalsIgnoreCase(monthStr)) {
        return 12;
      }
      return -1;
    }
    public String toString() {
      return "DATE(" + day + ", " + month + ", " + year + ")";
    }
    public String getSampleString() {
      return "(" + day + ", " + month + ", " + year + ")";
    }
    public Object get() {
      List<Schema.Field> fields = new ArrayList<Schema.Field>();
      fields.add(new Schema.Field("month", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("day", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("year", Schema.create(Schema.Type.INT), "", null));
      Schema newSchema = Schema.createRecord("date", "", "", false);
      newSchema.setFields(fields);
      GenericData.Record gdr = new GenericData.Record(newSchema);

      gdr.put("month", month);
      gdr.put("day", day);
      gdr.put("year", year);
      return gdr;
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
    public String getSampleString() {
      return "(" + hr + ", " + min + ", " + sec + ")";
    }
    public Object get() {
      List<Schema.Field> fields = new ArrayList<Schema.Field>();
      fields.add(new Schema.Field("hrs", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("mins", Schema.create(Schema.Type.INT), "", null));
      fields.add(new Schema.Field("secs", Schema.create(Schema.Type.INT), "", null));
      Schema newSchema = Schema.createRecord("timestamp", "", "", false);
      newSchema.setFields(fields);
      GenericData.Record gdr = new GenericData.Record(newSchema);

      gdr.put("hrs", hr);
      gdr.put("mins", min);
      gdr.put("secs", sec);
      return gdr;
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
    public String getSampleString() {
      return get().toString();
    }
    public Object get() {
      return i;
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
    public String getSampleString() {
      return get().toString();
    }
    public Object get() {
      return f;
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
    public String getSampleString() {
      return get().toString();
    }
    public Object get() {
      return new Utf8(s);
    }
  }

  static class EOLToken extends AbstractToken {
    public EOLToken() {
      super(EOL_TOKENCLASSID, null);
    }
    public String toString() {
      return "EOL()";
    }
    public String getSampleString() {
      return toString();
    }
    public Object get() {
      return null;
    }
  }

  static class WhitespaceToken extends AbstractToken {
    public WhitespaceToken() {
      super(WHITESPACE_TOKENCLASSID, null);
    }
    public String toString() {
      return "WS()";
    }
    public String getSampleString() {
      return toString();
    }
    public Object get() {
      return null;
    }
  }

  static class NoopToken extends AbstractToken {
    public NoopToken() {
      super(NOOP_TOKENCLASSID, null);
    }
    public String toString() {
      return "NOOP()";
    }
    public String getSampleString() {
      return toString();
    }
    public Object get() {
      return null;
    }
  }
}

