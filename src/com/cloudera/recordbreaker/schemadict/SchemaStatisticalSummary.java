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
package com.cloudera.recordbreaker.schemadict;

import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import java.util.Iterator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.lang.Math;
import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Text;

/********************************************
 * The SchemaStatistical Summary object is designed to mirror the structure of an input Schema.
 * In addition to the name and type information associated with a Schema object, it keeps statistical data
 * about observed actual data values that correspond to each Schema element.  
 *
 * This class is intended to be used in the following way:
 * 1) Instantiate a SchemaStatisticalSummary object with a preexisting Schema.
 * 2) For each GenericData item that exhibits the Schema, call SchemaStatisticalSummary.addData(GenericData).  This is
 *    designed to be called multiple times.
 * 3) Once all the desired data has been added, call finalizeStatistics().
 * 4) The resulting finalized SchemaStatisticalSummary object can then be compared to other SchemaStatisticalSummary objects with the measureDistance() function.
 *
 ********************************************/
public class SchemaStatisticalSummary implements Writable {
  static byte MAGIC = (byte) 0xa1;
  static byte VERSION = (byte) 1;

  final static double MATCHCOST_TYPE_CLASH = 1 * 10 * 1000;
  final static double MATCHCOST_CREATE = 1 * 1000;
  final static double MATCHCOST_DELETE = 1 * 1000;

  final static short ARRAY_NODE = 1;
  final static short BOOLEAN_NODE = 2;
  final static short BYTES_NODE = 3;
  final static short DOUBLE_NODE = 4;
  final static short ENUM_NODE = 5;
  final static short FIXED_NODE = 6;
  final static short FLOAT_NODE = 7;
  final static short INT_NODE = 8;
  final static short LONG_NODE = 9;
  final static short MAP_NODE = 10;
  final static short NULL_NODE = 11;
  final static short RECORD_NODE = 12;
  final static short STRING_NODE = 13;
  final static short UNION_NODE = 14;

  /////////////////////////////////////////////////
  // Inner classes
  /////////////////////////////////////////////////
  /*****************************************************
   * SummaryNode is a generic statistical summary object for a given elt in the
   * hierarchy.  A single tuple in the source data may yield a number of nested
   * SummaryNodes, all rooted at a GenericRecord.
   *
   * The hierarchy is instantiated by examining the schema.  Each new data item
   * results in a call to SummaryNode.addData(), in which the data item is passed in.
   ******************************************************/
  abstract class SummaryNode implements Cloneable {
    SummaryNode parent = null;
    int preorderIdx;
    int numData;
    String docStr = "";

    public SummaryNode() {
    }
    public SummaryNode(String docStr) {
      this.docStr = docStr;
    }
    //////////////////////////////////////////
    // Methods for constructing the summary-node tree
    //////////////////////////////////////////
    public void addData(Object obj) {
      if (obj instanceof Boolean) {
        this.addData((Boolean) obj);
      } else if (obj instanceof GenericArray) {
        this.addData((GenericArray) obj);
      } else if (obj instanceof Double) {
        this.addData((Double) obj);
      } else if (obj instanceof Float) {
        this.addData((Float) obj);
      } else if (obj instanceof GenericFixed) {
        this.addData((GenericFixed) obj);
      } else if (obj instanceof Integer) {
        this.addData((Integer) obj);
      } else if (obj instanceof Long) {
        this.addData((Long) obj);
      } else if (obj instanceof Map) {
        this.addData((Map) obj);
      } else if (obj instanceof ByteBuffer) {
        this.addData((ByteBuffer) obj);
      } else if (obj instanceof GenericRecord) {
        this.addData((GenericRecord) obj);
      } else if (obj instanceof Utf8) {
        this.addData((Utf8) obj);
      } else if (obj instanceof String) {
        this.addData((String) obj);
      }
    }
    // Overridden on per-subclass basis.
    public void addData(Boolean b) {};
    public void addData(GenericArray g) {};
    public void addData(Double d) {};
    public void addData(Float f) {};
    public void addData(Integer i) {};
    public void addData(Long l) {};
    public void addData(Map m) {};
    public void addData(ByteBuffer bb) {};
    public void addData(GenericRecord g) {};
    public void addData(Utf8 u) {};
    public void addData(String s) {};

    ///////////////////////////////////////////////
    // Tree-manipulation and info methods
    ///////////////////////////////////////////////
    /**
     * How many nodes in this subtree?
     */
    public int size() {
      int total = 0;
      for (SummaryNode child: children()) {
        total += child.size();
      }
      return total + 1;
    }    

    /**
     * Setters/getters
     */
    SummaryNode getParent() {
      return parent;
    }
    void setParent(SummaryNode parent) {
      this.parent = parent;
    }
    public List<SummaryNode> children() {
      return new ArrayList<SummaryNode>();
    }
    public int preorderCount() {
      return preorderIdx;
    }
    public SummaryNode parent() {
      return parent;
    }

    /**
     * Dealing with paths and node orderings
     */
    public int computePreorder(int lastIdx) {
      lastIdx++;
      this.preorderIdx = lastIdx;
      for (SummaryNode child: children()) {
        lastIdx = child.computePreorder(lastIdx);
        child.setParent(this);
      }
      return lastIdx;
    }
    void preorder(List<SummaryNode> soFar) {
      soFar.add(this);
      for (SummaryNode child: children()) {
        child.preorder(soFar);
      }
    }
    public List<SummaryNode> preorder() {
      List<SummaryNode> l = new ArrayList<SummaryNode>();
      preorder(l);
      return l;
    }
    public List<SummaryNode> pathToRoot() {
      List<SummaryNode> path = new ArrayList<SummaryNode>();
      SummaryNode cur = this;
      while (cur != null) {
        path.add(cur);
        cur = cur.getParent();
      }
      return path;
    }
    public List<SummaryNode> getLastNodeOnPath() {
      List<SummaryNode> path = new ArrayList<SummaryNode>();
      SummaryNode cur = this;
      while (cur != null) {
        path.add(cur);
        cur = cur.getParent();
      }
      return path;
    }

    ///////////////////////////////////////////////
    // Methods for string representation
    ///////////////////////////////////////////////
    /**
     * Helper method for rendering a string version of the data
     */
    String prefixString(int prefix) {
      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < prefix; i++) {
        buf.append(" ");
      }
      return buf.toString();
    }
    /**
     * Render a string version of the data
     */
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + "\n";
    }
    public abstract String getTypeDesc();
    /**
     * Find the right node and obtain a description of it.
     */
    public abstract String getDesc(boolean verbose);
    public String getDesc(int nodeid) {
      if (nodeid == preorderIdx) {
        return getDesc(false);
      } else {
        for (SummaryNode child: children()) {
          String desc = child.getDesc(nodeid);
          if (desc != null) {
            return desc;
          }
        }
      }
      return null;
    }
    public String getLabel(int nodeid) {
      if (nodeid == preorderIdx) {
        return getLabel();
      } else {
        for (SummaryNode child: children()) {
          String label = child.getLabel(nodeid);
          if (label != null) {
            return label;
          }
        }
      }
      return null;
    }
    public String getTypeDesc(int nodeid) {
      if (nodeid == preorderIdx) {
        return getTypeDesc();
      } else {
        for (SummaryNode child: children()) {
          String typedesc = child.getTypeDesc(nodeid);
          if (typedesc != null) {
            return typedesc;
          }
        }
      }
      return null;
    }
    public String getDocStr(int nodeid) {
      if (nodeid == preorderIdx) {
        return docStr;
      } else {
        for (SummaryNode child: children()) {
          String docstr = child.getDocStr(nodeid);
          if (docstr != null) {
            return docstr;
          }
        }
      }
      return null;
    }
    /**
     * Find the "label" for the current node.  Since the top-level element in the
     * NodeSummary hierarchy is a record, we know that every element has a label.
     * The getLabel() function goes up the tree to the root, constructing the 
     * dotted label sequence all the way.
     */
    public String getLabel() {
      if (parent != null) {
        return parent.getLabel("", this);
      } else {
        return "<root>";
      }
    }

    public String getLabel(String labelSoFar, SummaryNode src) {
      if (parent != null) {
        return parent.getLabel(labelSoFar, this);
      } else {
        return labelSoFar;
      }
    }

    ///////////////////////////////////////////////
    // Cost functions for schema matching
    ///////////////////////////////////////////////
    /**
     * Right now we do a REALLY brain-dead type-only schema matching.
     * We're collecting statistics on the input data; we just haven't yet hooked them up
     * to the transformation cost metric.
     */
    public double transformCost(SummaryNode other) {
      if (this.getClass() == other.getClass()) {
        return 0.5;
      } else {
        return MATCHCOST_TYPE_CLASH;
      }
    }
    public double deleteCost() {
      return MATCHCOST_DELETE;
    }
    public double createCost() {
      return MATCHCOST_CREATE;
    }

    ///////////////////////////////////////////////
    // Serialization/deserialization
    ///////////////////////////////////////////////
    public abstract void write(DataOutput out) throws IOException;
    public abstract void readFields(DataInput in) throws IOException;
  }

  /*****************************************************
   * Store statistical summary of observed arrays.  Basically, store length information and # times seen.
   ****************************************************/
  class ArraySummaryNode extends SummaryNode {
    int totalSize;
    SummaryNode eltSummary;
    public ArraySummaryNode() {
    }
    public ArraySummaryNode(SummaryNode eltSummary, String docStr) {
      super(docStr);
      this.eltSummary = eltSummary;
    }

    /**
     */
    public void addData(GenericArray data) {
      numData++;
      totalSize += data.size();
      for (Iterator it = data.iterator(); it.hasNext(); ) {
        eltSummary.addData(it.next());
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avgSize: " + (totalSize / (1.0 * numData)) + "\n" + eltSummary.dumpSummary(prefix+2);
    }
    public String getTypeDesc() {
      return "ARRAY";
    }
    public String getDesc(boolean verbose) {
      String desc = "ARRAY";
      if (verbose) {
        desc += "(numData: " + numData + ", avgSize: " + (totalSize / (1.0 * numData)) + ")";
      }
      return getLabel()  + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(ARRAY_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(totalSize);
      eltSummary.write(out);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.totalSize = in.readInt();
      this.eltSummary = readAndCreate(in);
    }
  }

  /*****************************************************
   * Store statistical summary of observed boolean field.  Store # times seen and distribution true vs false
   ****************************************************/
  class BooleanSummaryNode extends SummaryNode {
    int numTrue;
    int numFalse;
    public BooleanSummaryNode() {
    }
    public BooleanSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Boolean b) {
      numData++;
      if (b.booleanValue()) {
        numTrue++;
      } else {
        numFalse++;
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", numTrue: " + numTrue + ", numFalse: " + numFalse + "\n";
    }
    public String getTypeDesc() {
      return "BOOLEAN";
    }
    public String getDesc(boolean verbose) {
      String desc = "BOOLEAN";
      if (verbose) {
        desc += "(numData: " + numData + ", numTrue: " + numTrue + ", numFalse: " + numFalse + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(BOOLEAN_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(numTrue);
      out.writeInt(numFalse);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.numTrue = in.readInt();
      this.numFalse = in.readInt();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Bytes field.  Store # times seen and # bytes seen.
   ****************************************************/
  class BytesSummaryNode extends SummaryNode {
    int totalSize = 0;
    public BytesSummaryNode() {
    }
    public BytesSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(ByteBuffer bb) {
      numData++;
      totalSize += bb.remaining();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", totalSize: " + totalSize + "\n";
    }
    public String getTypeDesc() {
      return "BYTES";
    }
    public String getDesc(boolean verbose) {
      String desc = "BYTES";
      if (verbose) {
        desc += "(numData: " + numData + ", totalSize: " + totalSize + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(BYTES_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(totalSize);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.totalSize = in.readInt();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Double field.  Store # times seen and total value
   ****************************************************/
  class DoubleSummaryNode extends SummaryNode {
    double total;
    public DoubleSummaryNode() {
    }
    public DoubleSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Double d) {
      numData++;
      total += d.doubleValue();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "DOUBLE";
    }
    public String getDesc(boolean verbose) {
      String desc = "DOUBLE";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(DOUBLE_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeDouble(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readDouble();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Enumerated Type field.  Store # times seen and statistics on how often 
   * each enum-value is seen.
   ****************************************************/
  class EnumSummaryNode extends SummaryNode {
    String name;
    Map<String, Integer> symbolCounts = new HashMap<String, Integer>();
    public EnumSummaryNode() {
    }
    public EnumSummaryNode(String name, List<String> symbols, String docStr) {
      super(docStr);
      this.name = name;
      for (String symbol: symbols) {
        this.symbolCounts.put(symbol, 1);
      }
    }
    public void addData(String s) {
      this.symbolCounts.put(s, symbolCounts.get(s) + 1);
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      buf.append(prefixString(prefix) + "numData: " + numData + " =>\n");
      for (String symbol: symbolCounts.keySet()) {
        buf.append(prefixString(prefix+2) + symbol + ": " + symbolCounts.get(symbol) + "\n");
      }
      return buf.toString();
    }
    public String getTypeDesc() {
      return "ENUM";
    }
    public String getDesc(boolean verbose) {
      String desc = "ENUM";
      if (verbose) {
        desc += "(numData: " + numData + ", numSymbols: " + symbolCounts.size() + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(ENUM_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(symbolCounts.size());
      for (String symbol: symbolCounts.keySet()) {
        new Text(symbol).write(out);
        out.writeInt(symbolCounts.get(symbol));
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      symbolCounts = new HashMap<String, Integer>();
      int numElts = in.readInt();
      for (int i = 0; i < numElts; i++) {
        Text symbol = new Text();
        symbol.readFields(in);
        Integer count = in.readInt();
        symbolCounts.put(symbol.toString(), count);
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed GenericFixed field.  Store # times seen and byte length information.  Eventually,
   * store info on the byte content, too.
   ****************************************************/
  class FixedSummaryNode extends SummaryNode {
    String name;
    int size;
    int total;
    public FixedSummaryNode() {
    }
    public FixedSummaryNode(String name, int size, String docStr) {
      super(docStr);
      this.name = name;
      this.size = size;
      this.total = 0;
    }
    public void addData(GenericFixed data) {
      byte d[] = data.bytes();
      total += d.length;
      numData++;
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "size: " + size + ", total: " + total + ", numData: " + numData;
    }
    public String getTypeDesc() {
      return "FIXED";
    }
    public String getDesc(boolean verbose) {
      String desc = "FIXED";
      if (verbose) {
        desc += "(numData: " + numData + ", size: " + size + ", total: " + total + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(FIXED_NODE);
      new Text(name).write(out);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(size);
      out.writeInt(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.name = Text.readString(in);
      this.docStr = UTF8.readString(in);
      this.size = in.readInt();
      this.total = in.readInt();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Float field.  Store # times seen and total value
   ****************************************************/
  class FloatSummaryNode extends SummaryNode {
    float total;
    public FloatSummaryNode() {
    }
    public FloatSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Float f) {
      numData++;
      total += f.floatValue();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "FLOAT";
    }
    public String getDesc(boolean verbose) {
      String desc = "FLOAT";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(FLOAT_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeFloat(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readFloat();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Integer field.  Store # times seen and total value
   ****************************************************/
  class IntegerSummaryNode extends SummaryNode {
    int total;
    public IntegerSummaryNode() {
    }
    public IntegerSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Integer i) {
      numData++;
      total += i.intValue();
    }

    ///////////////////////////////////////////////
    // Cost functions for schema matching
    ///////////////////////////////////////////////
    public double transformCost(SummaryNode other) {
      if (this.getClass() == other.getClass()) {
        IntegerSummaryNode iother = (IntegerSummaryNode) other;
        double thisAvg = total / (1.0 * numData);
        double otherAvg = iother.total / (1.0 * iother.numData);

        // Currently, simply return difference of means as measure of integer set agreement.
        // This is a terrible measure that will fail for many comparisons (eg, it ignores 
        // distribution family entirely).  A placeholder for something better, soon.
        double cost = Math.abs(thisAvg - otherAvg);
        /**
        System.err.println("------------------------------------------------");
        System.err.println("LOCAL INT: " + dumpSummary(0) + " VS " + other.dumpSummary(0));
        System.err.println("INT compare: " + thisAvg + " vs " + otherAvg + ", cost: " + cost);
        **/
        return cost;
      } else {
        return MATCHCOST_TYPE_CLASH;
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "INT";
    }
    public String getDesc(boolean verbose) {
      String desc = "INT";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(INT_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, (docStr == null) ? "" : docStr);
      out.writeInt(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readInt();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Long field.  Store # times seen and total value
   ****************************************************/
  class LongSummaryNode extends SummaryNode {
    long total;
    public LongSummaryNode() {
    }
    public LongSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Long l) {
      numData++;
      total += l.longValue();
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg: " + (total / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "LONG";
    }
    public String getDesc(boolean verbose) {
      String desc = "LONG";
      if (verbose) {
        desc += "(numData: " + numData + ", avg: " + (total / (1.0 * numData)) + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(LONG_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeLong(total);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.total = in.readLong();
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Map field.  Store # times seen and track data for each labeled key-pair.
   ****************************************************/
  class MapSummaryNode extends SummaryNode {
    Schema modelS;
    HashMap<Utf8, SummaryNode> stats = new HashMap<Utf8, SummaryNode>();

    public MapSummaryNode() {
    }
    public MapSummaryNode(Schema modelS, String docStr) {
      super(docStr);
      this.modelS = modelS;
    }
    public void addData(Map m) {
      numData++;
      Iterator it = m.keySet().iterator();
      while (it.hasNext()) {
        Utf8 key = (Utf8) it.next();
        SummaryNode s = stats.get(key);
        if (s == null) {
          s = buildStructure(modelS, modelS.getDoc());
          stats.put(key, s);
        }
        s.addData(m.get(key));
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      buf.append(prefixString(prefix) + "numData: " + numData + "\n");
      for (Utf8 key: stats.keySet()) {
        SummaryNode s = stats.get(key);
        buf.append(prefixString(prefix) + key + " =>\n" + s.dumpSummary(prefix+2));
      }
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      return buf.toString();
    }
    public String getTypeDesc() {
      return "MAP";
    }
    public String getDesc(boolean verbose) {
      String desc = "MAP";
      if (verbose) {
        desc += "(numData: " + numData + ", numSymbols: " + stats.size() + ")";
      }
      return getLabel() + ": " + desc;
    }
    public String getLabel(String labelSoFar, SummaryNode src) {
      for (Utf8 fname: stats.keySet()) {
        SummaryNode candidate = stats.get(fname);
        if (src == candidate) {
          if (parent != null) {
            labelSoFar = (labelSoFar.length() > 0) ? fname.toString() + "." + labelSoFar : fname.toString();
            return parent.getLabel(labelSoFar, this);
          }
        }
      }
      return labelSoFar;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(MAP_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(stats.size());
      for (Utf8 key: stats.keySet()) {
        new Text(key.toString()).write(out);
        stats.get(key).write(out);
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      int numElts = in.readInt();
      for (int i = 0; i < numElts; i++) {
        Text key = new Text();
        key.readFields(in);
        SummaryNode sn = readAndCreate(in);
        stats.put(new Utf8(key.toString()), sn);
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Null field.  Just store # times seen.
   ****************************************************/
  class NullSummaryNode extends SummaryNode {
    public NullSummaryNode() {
    }
    public NullSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData() {
      numData++;
    }

    public String getDesc(boolean verbose) {
      String desc = "NULL";
      if (verbose) {
        desc += "(numData: " + numData + ")";
      }
      return getLabel() + ": " + desc;
    }
    public String getTypeDesc() {
      return "NULL";
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(NULL_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Record field.  Store # times seen and then data about sub-elements.
   ****************************************************/
  class RecordSummaryNode extends SummaryNode {
    String name;
    Map<String, SummaryNode> recordSummary = new HashMap<String, SummaryNode>();
    public RecordSummaryNode() {
    }
    public RecordSummaryNode(String name, String docStr) {
      super(docStr);
      this.name = name;
    }
    public List<SummaryNode> children() {
      List<SummaryNode> l = new ArrayList<SummaryNode>();
      for (String key: recordSummary.keySet()) {
        l.add(recordSummary.get(key));
      }
      return l;
    }
    public void addField(String fname, SummaryNode fn) {
      recordSummary.put(fname, fn);
    }
    public void addData(GenericRecord data) {
      numData++;
      for (String fname: recordSummary.keySet()) {
        recordSummary.get(fname).addData(data.get(fname));
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      buf.append(prefixString(prefix) + "numData: " + numData + "\n");
      for (String fname: recordSummary.keySet()) {
        buf.append(prefixString(prefix) + fname + " =>\n" + recordSummary.get(fname).dumpSummary(prefix+2));
      }
      buf.append(prefixString(prefix) + "+------------------------------------------+\n");
      return buf.toString();
    }
    public String getTypeDesc() {
      return "RECORD";
    }
    public String getDesc(boolean verbose) {
      String desc = "RECORD";
      if (verbose) {
        desc += "(numData: " + numData + ", fields: " + recordSummary.size() + ")";
      }
      return getLabel() + ": " + desc;
    }
    /**
    public String getLabel() {
      if (parent != null) {
        return parent.getLabel("", this);
      } else {
        return "<root>";
      }
    }
    **/
    public String getLabel(String labelSoFar, SummaryNode src) {
      for (String fname: recordSummary.keySet()) {
        SummaryNode candidate = recordSummary.get(fname);
        //System.err.println("FNAME: " + fname + ", " + candidate.getClass() + " vs " + src.getClass());
        if (src == candidate) {
          labelSoFar = (labelSoFar.length() > 0) ? fname + "." + labelSoFar : fname;
          if (parent != null) {
            return parent.getLabel(labelSoFar, this);
          } else {
            return "<root>" + "." + labelSoFar;
          }
        }
      }
      return "<root>" + "." + labelSoFar;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(RECORD_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(recordSummary.size());
      for (String fname: recordSummary.keySet()) {
        new Text(fname).write(out);
        recordSummary.get(fname).write(out);
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      int numRecs = in.readInt();
      for (int i = 0; i < numRecs; i++) {
        Text fname = new Text();
        fname.readFields(in);
        SummaryNode sn = readAndCreate(in);
        recordSummary.put(fname.toString(), sn);
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed String field.  Store # times seen and total length of the strings (for now).
   * Eventually, store info on the String content, too.
   ****************************************************/
  class StringSummaryNode extends SummaryNode {
    int totalLength;
    List<Utf8> observedStrings = new ArrayList<Utf8>();
    public StringSummaryNode() {
    }
    public StringSummaryNode(String docStr) {
      super(docStr);
    }
    public void addData(Utf8 s) {
      numData++;
      totalLength += s.getLength();
      observedStrings.add(s);
    }

    ///////////////////////////////////////////////
    // Cost functions for schema matching
    ///////////////////////////////////////////////
    public double transformCost(SummaryNode other) {
      if (this.getClass() == other.getClass()) {
        return 0.5;
      } else {
        return MATCHCOST_TYPE_CLASH;
      }      
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      return prefixString(prefix) + "numData: " + numData + ", avg-len: " + (totalLength / (1.0 * numData)) + "\n";
    }
    public String getTypeDesc() {
      return "STRING";
    }
    public String getDesc(boolean verbose) {
      String desc = "STRING";
      if (verbose) {
        desc += "(numData: " + numData + ", avglen: " + (totalLength / (1.0 * numData)) + ")";
      }
      return getLabel()  + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(STRING_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(totalLength);
      for (Utf8 s: observedStrings) {
        UTF8.writeString(out, s.toString());
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      this.totalLength = in.readInt();
      observedStrings.clear();
      for (int i = 0; i < numData; i++) {
        observedStrings.add(new Utf8(UTF8.readString(in)));
      }
    }    
  }

  /*****************************************************
   * Store statistical summary of observed Union field.  Actually, a Union is not observed directly - we just know
   * it's a union from the schema.  Store # times seen, data on the particular type observed, and statistics on how 
   * often each subtype is seen.
   ****************************************************/
  class UnionSummaryNode extends SummaryNode {
    Map<Schema.Type, SummaryNode> unionTypes = new HashMap<Schema.Type, SummaryNode>();
    Map<Schema.Type, Integer> unionTypeCounts = new HashMap<Schema.Type, Integer>();
    public UnionSummaryNode() {
    }
    public UnionSummaryNode(String docStr) {
      super(docStr);
    }
    public void addType(Schema.Type t, SummaryNode sn) {
      if (unionTypes.get(t) == null) {
        unionTypes.put(t, sn);
        unionTypeCounts.put(t, 0);
      }
    }

    /**
     * We need to dispatch the object to the right element stored in 'unionTypes'
     */
    public void addData(Object obj) {
      Schema.Type t = Schema.Type.ARRAY;
      if (obj instanceof GenericArray) {
        t = Schema.Type.ARRAY;
      } else if (obj instanceof Boolean) {
        t = Schema.Type.BOOLEAN;
      } else if (obj instanceof ByteBuffer) {
        t = Schema.Type.BYTES;
      } else if (obj instanceof Double) {
        t = Schema.Type.DOUBLE;
      } else if (obj instanceof String) {
        t = Schema.Type.ENUM;
      } else if (obj instanceof GenericFixed) {
        t = Schema.Type.FIXED;
      } else if (obj instanceof Float) {
        t = Schema.Type.FLOAT;
      } else if (obj instanceof Integer) {
        t = Schema.Type.INT;
      } else if (obj instanceof Long) {
        t = Schema.Type.LONG;
      } else if (obj instanceof Map) {
        t = Schema.Type.MAP;
      } else if (obj instanceof GenericRecord) {
        t = Schema.Type.RECORD;
      } else if (obj instanceof Utf8) {
        t = Schema.Type.STRING;
      }
      unionTypes.get(t).addData(obj);
      Integer c = unionTypeCounts.get(t);
      if (c == null) {
        unionTypeCounts.put(t, 1);
      } else {
        unionTypeCounts.put(t, c.intValue() + 1);
      }
    }

    /////////////////////////////
    // String representation
    /////////////////////////////
    public String dumpSummary(int prefix) {
      StringBuffer buf = new StringBuffer();
      for (Schema.Type t: unionTypes.keySet()) {
        buf.append(prefixString(prefix) + "unionType: " + t + " =>\n");
        buf.append(unionTypes.get(t).dumpSummary(prefix+2));
      }
      return buf.toString();
    }
    public String getTypeDesc() {
      return "UNION";
    }
    public String getDesc(boolean verbose) {
      String desc = "UNION";
      if (verbose) {
        desc += "(numData: " + numData + ", numtypes: " + unionTypes.size() + ")";
      }
      return getLabel() + ": " + desc;
    }

    /////////////////////////////
    // Serialize/deserialize
    /////////////////////////////
    public void write(DataOutput out) throws IOException {
      out.writeShort(UNION_NODE);
      out.writeInt(numData);
      UTF8.writeString(out, docStr == null ? "" : docStr);
      out.writeInt(unionTypes.size());
      for (Schema.Type t: unionTypes.keySet()) {
        new Text(t.toString()).write(out);
        out.writeInt(unionTypeCounts.get(t));
        unionTypes.get(t).write(out);
      }
    }
    public void readFields(DataInput in) throws IOException {
      this.numData = in.readInt();
      this.docStr = UTF8.readString(in);
      int numTypes = in.readInt();
      for (int i = 0; i < numTypes; i++) {
        Text tLabel = new Text();
        tLabel.readFields(in);
        Schema.Type t = Schema.Type.valueOf(tLabel.toString());
        int typeCount = in.readInt();
        SummaryNode sn = readAndCreate(in);
        unionTypes.put(t, sn);
        unionTypeCounts.put(t, typeCount);
      }
    }    
  }
  /***************************************
   * Op is used to track mapping results
   ***************************************/
  class PreviousChoice extends SchemaMappingOp {
    Hashtable<String, List<SchemaMappingOp>> h;
    String label;
    public PreviousChoice(Hashtable<String, List<SchemaMappingOp>> h, int i, int j) {
      this.h = h;
      this.label = "" + i + "-" + j;
    }
    public PreviousChoice(Hashtable<String, List<SchemaMappingOp>> h, int p1, int p2, int p3, int p4, int p5, int p6) {
      this.h = h;
      this.label = "" + p1 + "-" + p2 + "-" + p3 + "-" + p4 + "-" + p5 + "-" + p6;
    }
    public List<SchemaMappingOp> getOps() {
      List<SchemaMappingOp> ops = h.get(label);
      if (ops == null) {
        ops = new ArrayList<SchemaMappingOp>();
      }
      return ops;
    }
    public String toString() {
      return "Previous! " + label;
    }
  }


  /////////////////////////////////////////////////
  // Members
  /////////////////////////////////////////////////
  SummaryNode root = null;
  String datasetLabel = "";

  /////////////////////////////////////////////////
  // Constructors, initializers
  /////////////////////////////////////////////////
  public SchemaStatisticalSummary() throws IOException {
  }
  public SchemaStatisticalSummary(String datasetLabel) throws IOException {
    this.datasetLabel = datasetLabel;
  }

  /**
   * Create the statistical summary object from data.
   */
  public Schema createSummaryFromData(File f) throws IOException {
    DataFileReader in = new DataFileReader(f, new GenericDatumReader());
    try {
      Schema s = in.getSchema();

      //
      // There has to be at least one data element for us to infer anything meaningful
      //
      Iterator it = in.iterator();
      if (! it.hasNext()) {
        throw new IOException("No contents");
      }

      //
      // We can only infer schemas from top-level records, not Fixeds or Arrays.
      //
      Object firstRecord = it.next();
      if (firstRecord instanceof GenericFixed ||
          firstRecord instanceof GenericArray) {
        throw new IOException("Not a top-level record");
      }

      // We assume the passed-in top-level Schema always represents a Record.
      if (s.getType() != Schema.Type.RECORD) {
        throw new IOException("Passed-in top-level Schema instance must be of type Schema.Type.RECORD");
      }
      this.root = buildStructure(s, "ROOT");

      //
      // Iterate through all records and collect statistics on each Schema field.
      //
      List<Schema.Field> fields = s.getFields();
      GenericRecord cur = (GenericRecord) firstRecord;
      int counter = 0;
      do {
        this.root.addData(cur);
        counter++;
        if (it.hasNext()) {
          cur = (GenericRecord) it.next();
        } else {
          cur = null;
        }
      } while (cur != null);

      this.root.computePreorder(0);
      return s;
    } finally {
      in.close();
    }
  }

  /**
   * This function reads in data and instantiates the SummaryNode hierarchy.
   */
  public SummaryNode readAndCreate(DataInput in) throws IOException {
    short nodeType = in.readShort();
    SummaryNode sn = null;

    switch (nodeType) {
    case ARRAY_NODE: {
      sn = new ArraySummaryNode();
      break;
    }
    case BOOLEAN_NODE: {
      sn = new BooleanSummaryNode();
      break;
    }
    case BYTES_NODE: {
      sn = new BytesSummaryNode();
      break;
    }
    case DOUBLE_NODE: {
      sn = new DoubleSummaryNode();
      break;
    }
    case ENUM_NODE: {
      sn = new EnumSummaryNode();
      break;
    }
    case FIXED_NODE: {
      sn = new FixedSummaryNode();
      break;
    }
    case FLOAT_NODE: {
      sn = new FloatSummaryNode();
      break;
    }
    case INT_NODE: {
      sn = new IntegerSummaryNode();
      break;
    }
    case LONG_NODE: {
      sn = new LongSummaryNode();
      break;
    }
    case MAP_NODE: {
      sn = new MapSummaryNode();
      break;
    }
    case NULL_NODE: {
      sn = new NullSummaryNode();
      break;
    }
    case RECORD_NODE: {
      sn = new RecordSummaryNode();
      break;
    }
    case STRING_NODE: {
      sn = new StringSummaryNode();
      break;
    }
    case UNION_NODE: {
      sn = new UnionSummaryNode();
      break;
    }
    default:
      throw new IOException("Unknown node type: " + nodeType);
    }

    sn.readFields(in);
    return sn;
  }

  /**
   * Build a Summary structure out of the given schema.  Helper method.
   */ 
  SummaryNode buildStructure(Schema s, String docStr) {
    Schema.Type stype = s.getType();
    if (stype == Schema.Type.ARRAY) {
      return new ArraySummaryNode(buildStructure(s.getElementType(), s.getDoc()), docStr);
    } else if (stype == Schema.Type.BOOLEAN) {
      return new BooleanSummaryNode(docStr);
    } else if (stype == Schema.Type.BYTES) {
      return new BytesSummaryNode(docStr);
    } else if (stype == Schema.Type.DOUBLE) {
      return new DoubleSummaryNode(docStr);
    } else if (stype == Schema.Type.ENUM) {
      return new EnumSummaryNode(s.getFullName(), s.getEnumSymbols(), docStr);
    } else if (stype == Schema.Type.FIXED) {
      return new FixedSummaryNode(s.getFullName(), s.getFixedSize(), docStr);
    } else if (stype == Schema.Type.FLOAT) {
      return new FloatSummaryNode(docStr);
    } else if (stype == Schema.Type.INT) {
      return new IntegerSummaryNode(docStr);
    } else if (stype == Schema.Type.LONG) {
      return new LongSummaryNode(docStr);
    } else if (stype == Schema.Type.MAP) {
      return new MapSummaryNode(s.getValueType(), docStr);
    } else if (stype == Schema.Type.NULL) {
      return new NullSummaryNode(docStr);
    } else if (stype == Schema.Type.RECORD) {
      RecordSummaryNode rsn = new RecordSummaryNode(s.getFullName(), docStr);
      for (Field f: s.getFields()) {
        rsn.addField(f.name(), buildStructure(f.schema(), f.doc()));
      }
      return rsn;
    } else if (stype == Schema.Type.STRING) {
      return new StringSummaryNode(docStr);
    } else if (stype == Schema.Type.UNION) {
      UnionSummaryNode usn = new UnionSummaryNode(docStr);
      for (Schema subschema: s.getTypes()) {
        usn.addType(subschema.getType(), buildStructure(subschema, subschema.getDoc()));
      }
    }
    return null;
  }

  /////////////////////////////////////////////////////////
  // Schema distance computation
  /////////////////////////////////////////////////////////
  /**
   * Compute a distance between two statistical summaries.  
   * Larger values indicate trees that are more different.
   *
   * This value should not be interpreted as a probability.  It's just a score.
   *
   * It is very roughly the schema-edit-distance.  We transform one schema into another
   * through a series of node-edits, inserts, deletes.  The cost of a node-edit depends
   * on the similarity of the nodes involved.  The algorithm we use is 
   * similar to that described in "The Tree-to-Tree Correction Problem", Tai, JACM, 1979.
   */
  public SchemaMapping getBestMapping(SchemaStatisticalSummary other) {
    SummaryNode t1 = root;
    SummaryNode t2 = other.root;

    ////////////////////////////////////////
    // Part I of tree-correction algorithm
    ////////////////////////////////////////
    Hashtable E = new Hashtable();    
    Hashtable<String, List<SchemaMappingOp>> Echoice = new Hashtable<String, List<SchemaMappingOp>>();
    for (SummaryNode iNode: t1.preorder()) {
      int iIdx = iNode.preorderCount();
      for (SummaryNode jNode: t2.preorder()) {
        int jIdx = jNode.preorderCount();

        SummaryNode xNode = null;
        int xIdx = -1;
        for (SummaryNode uNode:  iNode.pathToRoot()) {
          int uIdx = uNode.preorderCount();

          for (SummaryNode sNode: uNode.pathToRoot()) {
            int sIdx = sNode.preorderCount();

            SummaryNode yNode = null;
            int yIdx = -1;
            for (SummaryNode vNode: jNode.pathToRoot()) {
              int vIdx = vNode.preorderCount();
              for (SummaryNode tNode: vNode.pathToRoot()) {
                int tIdx = tNode.preorderCount();

                if (((sIdx == uIdx) && (uIdx == iIdx)) &&
                    ((tIdx == vIdx) && (vIdx == jIdx))) {
                  storeValue(E, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx, 
                             iNode.transformCost(jNode));

                  //System.err.println("TXCOST: " + iNode.transformCost(jNode));

                  // The choice is TRANSFORM-I-J
                  storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                              new SchemaMappingOp(SchemaMappingOp.TRANSFORM_OP, this, iIdx, other, jIdx));
                } else if (((sIdx == uIdx) && (uIdx == iIdx)) ||
                           ((tIdx < vIdx) && (vIdx == jIdx))) {
                  storeValue(E, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx, 
                             getValue(E, sIdx, uIdx, iIdx, tIdx, jNode.parent().preorderCount(), jIdx - 1) +
                             jNode.createCost());
                  // The choice is CREATE-J plus a previous one!
                  storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                              new SchemaMappingOp(SchemaMappingOp.CREATE_OP, other, jIdx));
                  storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                              new PreviousChoice(Echoice, sIdx, uIdx, iIdx, tIdx, jNode.parent().preorderCount(), jIdx - 1));
                } else if (((sIdx < uIdx) && (uIdx == iIdx)) ||
                           ((tIdx == vIdx) && (vIdx == jIdx))) {
                  storeValue(E, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx, 
                             getValue(E, sIdx, iNode.parent().preorderCount(), iIdx-1, tIdx, vIdx, jIdx) +
                             iNode.deleteCost());
                  // The choice is DELETE-I plus a previous one!
                  storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                              new SchemaMappingOp(SchemaMappingOp.DELETE_OP, this, iIdx));
                  storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                              new PreviousChoice(Echoice, sIdx, iNode.parent().preorderCount(), iIdx-1, tIdx, vIdx, jIdx));
                } else {
                  // The choice is some combination of the previous ones!
                  double option1 = getValue(E, sIdx, xIdx, iIdx, tIdx, vIdx, jIdx);
                  double option2 = getValue(E, sIdx, uIdx, iIdx, tIdx, yIdx, jIdx);
                  double option3 = getValue(E, sIdx, uIdx, xIdx-1, tIdx, vIdx, yIdx-1) +
                    getValue(E, xIdx, xIdx, iIdx, yIdx, yIdx, jIdx);
                  double min = Math.min(option1, Math.min(option2, option3));
                  storeValue(E, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx, min);

                  if (option1 == min) {
                    storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                                new PreviousChoice(Echoice, sIdx, xIdx, iIdx, tIdx, vIdx, jIdx));
                  } else if (option2 == min) {
                    storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                                new PreviousChoice(Echoice, sIdx, uIdx, iIdx, tIdx, yIdx, jIdx));
                  } else if (option3 == min) {
                    storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                                new PreviousChoice(Echoice, sIdx, uIdx, xIdx-1, tIdx, vIdx, yIdx-1));
                    storeChoice(Echoice, sIdx, uIdx, iIdx, tIdx, vIdx, jIdx,
                                new PreviousChoice(Echoice, xIdx, xIdx, iIdx, yIdx, yIdx, jIdx));
                  }
                }
              }
              yNode = vNode;
              yIdx = yNode.preorderCount();
            }
          }
          xNode = uNode;
          xIdx = xNode.preorderCount();
        }
      }
    }
    ////////////////////////////////////////
    // Part II of tree-correction algorithm
    ////////////////////////////////////////
    double minM[][] = new double[t1.size()+1][];
    Hashtable<String, List<SchemaMappingOp>> Mchoice = new Hashtable<String, List<SchemaMappingOp>>();

    for (int i = 0; i < minM.length; i++) {
      minM[i] = new double[t2.size()+1];
    }
    minM[1][1] = 0;
    storeChoice(Mchoice, 1, 1, new SchemaMappingOp(SchemaMappingOp.TRANSFORM_OP, this, 1, other, 1));

    for (SummaryNode iNode: t1.preorder()) {
      int iIdx = iNode.preorderCount();
      if (iIdx < 2) {
        continue;
      }
      minM[iIdx][1] = minM[iIdx-1][1] + iNode.deleteCost();
      storeChoice(Mchoice, iIdx, 1, new PreviousChoice(Mchoice, iIdx-1, 1));
      storeChoice(Mchoice, iIdx, 1, new SchemaMappingOp(SchemaMappingOp.DELETE_OP, this, iIdx));
    }
    for (SummaryNode jNode: t2.preorder()) {
      int jIdx = jNode.preorderCount();
      if (jIdx < 2) {
        continue;
      }
      minM[1][jIdx] = minM[1][jIdx-1] + jNode.createCost();
      storeChoice(Mchoice, 1, jIdx, new PreviousChoice(Mchoice, 1, jIdx-1));
      storeChoice(Mchoice, 1, jIdx, new SchemaMappingOp(SchemaMappingOp.CREATE_OP, other, jIdx));
    }

    for (SummaryNode iNode: t1.preorder()) {
      int iIdx = iNode.preorderCount();
      if (iNode.preorderCount() < 2) {
        continue;
      }
      for (SummaryNode jNode: t2.preorder()) {
        int jIdx = jNode.preorderCount();
        if (jNode.preorderCount() < 2) {
          continue;
        }

        SummaryNode chosenSNode = null;
        SummaryNode chosenTNode = null;

        minM[iIdx][jIdx] = Double.MAX_VALUE;
        List<SchemaMappingOp> curBestOps = new ArrayList<SchemaMappingOp>();

        for (SummaryNode sNode: iNode.parent().pathToRoot()) {
          int sIdx = sNode.preorderCount();
          for (SummaryNode tNode: jNode.parent().pathToRoot()) {          
            int tIdx = tNode.preorderCount();
            double tmp = minM[sIdx][tIdx] +
              getValue(E, sIdx, iNode.parent().preorderCount(), iIdx - 1,
                       tIdx, jNode.parent().preorderCount(), jIdx - 1) - 
              sNode.transformCost(tNode);

            //
            // REMIND - mjc - I'm not sure this code correctly translates the min cost into 
            // an appropriate operation reconstruction.  This appears to be where we sometimes incorrectly
            // pursue a delete/create strategy instead of a lower-cost transformation-based one.
            //
            if (tmp <= minM[iIdx][jIdx]) {
              minM[iIdx][jIdx] = tmp;
              curBestOps.clear();
              // We substract the s-t transformation cost from the "tmp" computation above, as the other terms count it
              // twice.  Thus, the operation-generation code must substract a TRANSFORM out of the op-stream.
              // This is why the code adds a "Negative Transformation".  It will be used to cancel out a TRANSFORM
              // in the final operation stream later on.
              curBestOps.add(new PreviousChoice(Mchoice, sIdx, tIdx));
              curBestOps.add(new PreviousChoice(Echoice, 
                                                sIdx, iNode.parent().preorderCount(), iIdx - 1,
                                                tIdx, jNode.parent().preorderCount(), jIdx - 1));
              curBestOps.add(new SchemaMappingOp(SchemaMappingOp.NEGATIVE_TRANSFORM_OP, this, sIdx, other, tIdx));
            }
            // The current possible choice is OPS-FROM-MIN(S, T) PLUS OPS-FROM-E(s, i-1, t, j-1)
          }
        }
        //System.err.print("Transform cost: " + iNode.transformCost(jNode) + " for " + iIdx + ", " + jIdx + ", with pre-tx cost " + minM[iIdx][jIdx]);
        minM[iIdx][jIdx] = minM[iIdx][jIdx] + iNode.transformCost(jNode);
        //System.err.println("... post of " + minM[iIdx][jIdx]);

        // The choice is TRANSFORM-I-J PLUS ops from previous step
        for (SchemaMappingOp op: curBestOps) {
          storeChoice(Mchoice, iIdx, jIdx, op);
        }
        storeChoice(Mchoice, iIdx, jIdx,
                    new SchemaMappingOp(SchemaMappingOp.TRANSFORM_OP, this, iIdx, other, jIdx));
      }
    }

    ////////////////////////////////////////
    // Part III of tree-correction algorithm
    ////////////////////////////////////////
    double D[][] = new double[t1.size()+1][];
    Hashtable<String, List<SchemaMappingOp>> Dchoice = new Hashtable<String, List<SchemaMappingOp>>();

    int mapping[][] = new int[t1.size()+1][];
    for (int i = 0; i < D.length; i++) {
      D[i] = new double[t2.size()+1];
      mapping[i] = new int[t2.size()+1];
    }

    D[1][1] = 0;
    storeChoice(Dchoice, 1, 1, new SchemaMappingOp(SchemaMappingOp.TRANSFORM_OP, this, 1, other, 1));


    for (SummaryNode iNode: t1.preorder()) {
      int i = iNode.preorderCount();
      if (i < 2) {
        continue;
      }
      D[i][1] = D[i-1][1] + iNode.deleteCost();
      // The decision is DELETE-I PLUS OPS-FROM-D[i-1][j]
      storeChoice(Dchoice, i, 1, 
                  new SchemaMappingOp(SchemaMappingOp.DELETE_OP, this, i));
      storeChoice(Dchoice, i, 1, 
                  new PreviousChoice(Dchoice, i-1, 1));
    }
    for (SummaryNode jNode: t2.preorder()) {
      int j = jNode.preorderCount();
      if (j < 2) {
        continue;
      }
      D[1][j] = D[1][j-1] + jNode.createCost();
      // The decision is CREATE-J PLUS OPS-FROM-D[i][j-1]
      storeChoice(Dchoice, 1, j,
                  new SchemaMappingOp(SchemaMappingOp.CREATE_OP, other, j));
      storeChoice(Dchoice, 1, j,
                  new PreviousChoice(Dchoice, 1, j-1));
    }

    for (SummaryNode iNode: t1.preorder()) {
      int i = iNode.preorderCount();
      if (i < 2) {
        continue;
      }
      for (SummaryNode jNode: t2.preorder()) {
        int j = jNode.preorderCount();
        if (j < 2) {
          continue;
        }
        double option1 = D[i][j-1] + jNode.createCost();
        double option2 = D[i-1][j] + iNode.deleteCost();
        double option3 = minM[i][j];
        double min = Math.min(option1, Math.min(option2, option3));
        D[i][j] = min;

        // The choice is one of:
        // 1) CREATE-J + OPS-FROM-D[i][j-1], or
        // 2) DELETE-I + OPS-FROM-D[i-1][j], or 
        // 3) OPS-FROM-M
        if (option1 == min) {
          storeChoice(Dchoice, i, j,
                      new SchemaMappingOp(SchemaMappingOp.CREATE_OP, other, j));
          storeChoice(Dchoice, i, j,
                      new PreviousChoice(Dchoice, i, j-1));
        } else if (option2 == min) {
          storeChoice(Dchoice, i, j,
                      new SchemaMappingOp(SchemaMappingOp.DELETE_OP, this, i));
          storeChoice(Dchoice, i, j,
                      new PreviousChoice(Dchoice, i-1, j));
        } else {
          storeChoice(Dchoice, i, j,
                      new PreviousChoice(Mchoice, i, j));
        }
      }
    }

    //
    // Now massage the mapping until only base-level elements remain
    //
    int maxI = D.length-1;
    int maxJ = D[0].length-1;
    List<SchemaMappingOp> bestOps = Dchoice.get("" + maxI + "-" + maxJ);
    boolean maybeHasPrev = true;
    int counter = 0;
    while (maybeHasPrev) {
      maybeHasPrev = false;
      List<SchemaMappingOp> newOps = new ArrayList<SchemaMappingOp>();
      //System.err.println("ROUND " + counter);
      for (SchemaMappingOp op: bestOps) {
        //System.err.println("OP: " + op);
        if (op instanceof PreviousChoice) {
          //System.err.print("  ==> YIELDS ==> ");
          //for (SchemaMappingOp op2: ((PreviousChoice) op).getOps()) {
          //System.err.print(" " + op2 + ", ");
          //}
          //System.err.println();
          newOps.addAll(((PreviousChoice) op).getOps());
          maybeHasPrev = true;
        } else {
          newOps.add(op);
        }
      }
      //System.err.println("  Added " + newOps.size() + " ops.");
      //for (SchemaMappingOp curOp: newOps) {
      //System.err.println("    " + curOp);
      //}
      bestOps = newOps;
      counter++;
      //System.err.println();
    }

    // Finally, resolve "negative" operations
    List<SchemaMappingOp> negativeOps = new ArrayList<SchemaMappingOp>();
    for (Iterator<SchemaMappingOp> it = bestOps.iterator(); it.hasNext(); ) {
      SchemaMappingOp op = it.next();
      if (op.getOpcode() == SchemaMappingOp.NEGATIVE_TRANSFORM_OP) {
        it.remove();
        negativeOps.add(op);
      }
    }
    for (SchemaMappingOp negativeOp: negativeOps) {
      for (Iterator<SchemaMappingOp> it = bestOps.iterator(); it.hasNext(); ) {      
        SchemaMappingOp candidate = it.next();
        if (candidate.getOpcode() == SchemaMappingOp.TRANSFORM_OP &&
            candidate.getNodeId1() == negativeOp.getNodeId1() &&
            candidate.getNodeId2() == negativeOp.getNodeId2()) {
          it.remove();
          break;
        }
      }
    }

    // Distance of best mapping is stored in D[max-i][max-j].
    return new SchemaMapping(this, other, D[D.length-1][D[0].length-1], bestOps);
  }

  //////////////////////////////////////////////////////////
  // Utility methods for computing a schema mapping distance
  //////////////////////////////////////////////////////////
  double getValue(Hashtable E, int p1, int p2, int p3, int p4, int p5, int p6) {
    String label = "" + p1 + "-" + p2 + "-" + p3 + "-" + p4 + "-" + p5 + "-" + p6;
    return ((Double) E.get(label)).doubleValue();
  }
  void storeValue(Hashtable E, int p1, int p2, int p3, int p4, int p5, int p6, double cToStore) {
    String label = "" + p1 + "-" + p2 + "-" + p3 + "-" + p4 + "-" + p5 + "-" + p6;
    E.put(label, cToStore);
  }
  void storeChoice(Hashtable<String, List<SchemaMappingOp>> h, int i, int j, SchemaMappingOp op) {
    String label = "" + i + "-" + j;
    storeChoice(h, label, op);
  }
  void storeChoice(Hashtable<String, List<SchemaMappingOp>> h, int p1, int p2, int p3, int p4, int p5, int p6, SchemaMappingOp op) {
    String label = "" + p1 + "-" + p2 + "-" + p3 + "-" + p4 + "-" + p5 + "-" + p6;
    storeChoice(h, label, op);
  }
  void storeChoice(Hashtable<String, List<SchemaMappingOp>> h, String label, SchemaMappingOp op) {
    List<SchemaMappingOp> ops = h.get(label);
    if (ops == null) {
      ops = new ArrayList<SchemaMappingOp>();
      h.put(label, ops);
    }
    ops.add(op);
  }

  ////////////////////////////////////////////////
  // String representation of the overall summary object
  ////////////////////////////////////////////////
  public String getDatasetLabel() {
    return datasetLabel;
  }
  public String dumpSummary() {
    return this.root.dumpSummary(0);
  }
  public String getDesc(int nodeid) {
    return root.getDesc(nodeid);
  }
  public String getLabel(int nodeid) {
    return root.getLabel(nodeid);
  }
  public String getTypeDesc(int nodeid) {
    return root.getTypeDesc(nodeid);
  }
  public String getDocStr(int nodeid) {
    return root.getDocStr(nodeid);
  }

  ////////////////////////////////////////////////
  // Serialization/deserialization
  ////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.write(MAGIC);
    out.write(VERSION);
    root.write(out);
    UTF8.writeString(out, datasetLabel);
  }

  public void readFields(DataInput in) throws IOException {
    byte magic = in.readByte();
    byte version = in.readByte();
    this.root = readAndCreate(in);
    this.root.computePreorder(0);
    this.datasetLabel = UTF8.readString(in);
  }
}