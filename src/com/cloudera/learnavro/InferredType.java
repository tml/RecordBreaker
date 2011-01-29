// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.lang.reflect.*;
import java.util.*;
import java.io.*;
import java.util.regex.*;
import org.apache.hadoop.io.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericContainer;
import org.codehaus.jackson.JsonNode;

/*********************************************************
 * InferredType is returned by TypeInference.infer() for the inferred record type
 * of a file's contents.  It has several subclasses.
 *********************************************************/
public abstract class InferredType implements Writable {
  static byte BASE_TYPE = 1;
  static byte STRUCT_TYPE = 2;
  static byte ARRAY_TYPE = 3;
  static byte UNION_TYPE = 4;
  static double CARD_COST = Math.log(24);
  String name;

  /**
   * Super constructor
   */
  public InferredType() {
    name = createName();
  }
  abstract String createName();

  /**
   * Deserialize an unknown InferredType from the given input stream
   */
  public static InferredType readType(DataInput in) throws IOException {
    InferredType it = null;
    byte b = in.readByte();
    if (b == BASE_TYPE) {
      it = new BaseType();
    } else if (b == STRUCT_TYPE) {
      it = new StructType();
    } else if (b == ARRAY_TYPE) {
      it = new ArrayType();
    } else if (b == UNION_TYPE) {
      it = new UnionType();
    } else {
      throw new IOException("No type found: " + b);
    }
    it.readFields(in);
    return it;
  }

  /**
   * Use the type tree to parse the given input string
   */
  class ParseResult {
    Object data;
    boolean hasData;
    String s;

    /**
     */
    ParseResult(Object data, boolean hasData, String s) {
      this.data = data;
      this.hasData = hasData;
      this.s = s;
    }
    String getRemainingString() {
      return s;
    }
    Object getData() {
      return data;
    }
    boolean hasData() {
      return hasData;
    }
  }
  public GenericContainer parse(String str) {
    ParseResult pr = internalParse(str);
    if (pr.hasData() && pr.getRemainingString().length() == 0) {
      return (GenericContainer) pr.getData();
    } else {
      return null;
    }
  }
  abstract ParseResult internalParse(String s);

  /**
   * Read/write to disk
   */
  public abstract void readFields(DataInput in) throws IOException;
  public abstract void write(DataOutput out) throws IOException;

  /**
   * Used for auto-refining the type description
   */
  public abstract double getDescriptionCost();
  //public abstract InferredType refine(RefinementRule rules[]);

  /**
   * Accessors
   */
  public abstract Schema getAvroSchema();
  public abstract InferredType hoistUnions();
  public String getName() {
    return name;
  }
  public JsonNode getDefaultValue() {
    return null;
  }
}

/***************************************
 * A BaseType represents a bottom-level parsable object: string, int, ipaddr, date, etc.
 ***************************************/
class BaseType extends InferredType {
  Class baseTokenClass;
  String baseSuffix;
  String baseTokenIdentifier;
  
  Schema avroSchema;
  boolean hasData;
  static int fieldCounter = 0;

  public BaseType() {
  }
  public BaseType(Class baseTokenClass, String baseSuffix) {
    this.baseTokenClass = baseTokenClass;
    this.baseSuffix = baseSuffix;
    init();
  }
  void init() {
    try {
      baseTokenIdentifier = (String) baseTokenClass.getField("tokenTypeLabel").get(null) + baseSuffix;
    } catch (NoSuchFieldException nsfe) {
      baseTokenIdentifier = baseTokenClass.toString() + baseSuffix;
      nsfe.printStackTrace();
    } catch (IllegalAccessException iae) {
      baseTokenIdentifier = baseTokenClass.toString() + baseSuffix;
      iae.printStackTrace();
    }
    try {
      hasData = ((Boolean) baseTokenClass.getField("hasData").get(null)).booleanValue();
    } catch (NoSuchFieldException nsfe) {
      baseTokenIdentifier = baseTokenClass.toString() + baseSuffix;
      nsfe.printStackTrace();
    } catch (IllegalAccessException iae) {
      baseTokenIdentifier = baseTokenClass.toString() + baseSuffix;
      iae.printStackTrace();
    }
    try {
      avroSchema = (Schema) baseTokenClass.getMethod("createAvroSchema", null).invoke(null, null);
    } catch (NoSuchMethodException nsme) {
      nsme.printStackTrace();
    } catch (InvocationTargetException ite) {
      ite.printStackTrace();
    } catch (IllegalAccessException iae) {
      iae.printStackTrace();
    }
  }
  public InferredType hoistUnions() {
    return this;
  }
  public Schema getAvroSchema() {
    return avroSchema;
  }
  ParseResult internalParse(String s) {
    Matcher m = Tokenizer.intPattern.matcher(s);
    if (hasData && m.lookingAt()) {
      int lastGroupChar = m.end(m.groupCount());
      String newS = "";
      if (s.length() > lastGroupChar) {
        newS = s.substring(lastGroupChar);
      }
      Integer i = null;
      try {
        i = new Integer(Integer.parseInt(m.group(1)));
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
      return new ParseResult(i, true, newS);
    } else {
      s = s.substring(1);
      return new ParseResult(null, false, s);
    }
  }
  public String toString() {
    return "Base: " + baseTokenIdentifier + " ";
  }
  public double getDescriptionCost() {
    return CARD_COST;
  }
  String createName() {
    return "base-" + fieldCounter++;
  }
  public void readFields(DataInput in) throws IOException {
    String cname = UTF8.readString(in);
    try {
      this.baseTokenClass = Class.forName(cname);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException("Could not init deserialized classname: " + cname);
    }
    this.baseSuffix = UTF8.readString(in);
    init();
  }
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, baseTokenClass.getName());
    UTF8.writeString(out, baseSuffix);
  }
  //public InferredType refine(RefinementRule rules[]) {
  //InferredType candidates[] = new InferredType[rules.length];
//}
}

/***************************************
 * Represents a record
 ****************************************/
class StructType extends InferredType {
  List<InferredType> structTypes;
  static int recordCounter = 0;

  public StructType() {
  }
  public StructType(List<InferredType> structTypes) {
    this.structTypes = structTypes;
  }
  public InferredType hoistUnions() {
    List<InferredType> newStructTypes = new ArrayList<InferredType>();
    for (InferredType it: structTypes) {
      newStructTypes.add(it.hoistUnions());
    }
    return new StructType(newStructTypes);
  }
  public Schema getAvroSchema() {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (InferredType it: structTypes) {
      Schema itS = it.getAvroSchema();
      if (itS == null) {
        continue;
      }
      fields.add(new Schema.Field(it.getName(), it.getAvroSchema(), "", it.getDefaultValue()));
    }
    Schema s = Schema.createRecord(name, "", "", false);
    s.setFields(fields);
    return s;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("Struct: (");
    for (InferredType it: structTypes) {
      buf.append(it.toString() + ", ");
    }
    buf.append(") ");
    return buf.toString();
  }
  public double getDescriptionCost() {
    double dc = CARD_COST;
    for (InferredType it: structTypes) {
      dc += it.getDescriptionCost();
    }
    return dc;
  }
  String createName() {
    return "record-" + recordCounter++;
  }
  public void readFields(DataInput in) throws IOException {
    int numStructTypes = in.readInt();
    structTypes = new ArrayList<InferredType>();
    for (int i = 0; i < numStructTypes; i++) {
      structTypes.add(InferredType.readType(in));
    }
  }
  public void write(DataOutput out) throws IOException {
    out.write(STRUCT_TYPE);
    out.writeInt(structTypes.size());
    for (InferredType it: structTypes) {
      it.write(out);
    }
  }

  /**
   * Parse the given string, return resulting data if appropriate.
   */
  ParseResult internalParse(String s) {
    boolean hasData = false;
    Schema localSchema = getAvroSchema();
    GenericData.Record gdr = new GenericData.Record(localSchema);
    String currentStr = s;

    for (InferredType subelt: structTypes) {
      if (currentStr.length() == 0) {
        break;
      }
      ParseResult pr = subelt.internalParse(currentStr);
      if (pr == null) {
        return null;
      }
      if (pr.hasData()) {
        hasData = true;
        gdr.put(subelt.getName(), pr.getData());
      }
      currentStr = pr.getRemainingString();
    }
    return new ParseResult(gdr, hasData, currentStr);
  }
}

class ArrayType extends InferredType {
  InferredType bodyType;
  static int arrayCounter = 0;

  public ArrayType() {
  }
  public ArrayType(InferredType bodyType) {
    this.bodyType = bodyType;
  }
  public InferredType hoistUnions() {
    return new ArrayType(bodyType.hoistUnions());
  }
  public Schema getAvroSchema() {
    Schema s = Schema.createArray(bodyType.getAvroSchema());
    return s;
  }
  public String toString() {
    return "Array: (" + bodyType.toString() + ") ";
  }
  public double getDescriptionCost() {
    return CARD_COST + bodyType.getDescriptionCost();
  }
  String createName() {
    return "array-" + arrayCounter++;
  }
  /**
   * Parse the given string, return resulting data if appropriate.
   */
  ParseResult internalParse(String s) {
    boolean hasData = false;
    Schema localSchema = getAvroSchema();
    GenericData.Array gda = new GenericData.Array(5, localSchema);
    String currentStr = s;

    while (true) {
      ParseResult pr = bodyType.internalParse(s);
      if (pr == null) {
        break;
      }
      assert(pr.hasData());

      gda.add(pr.getData());
      currentStr = pr.getRemainingString();
    }
    return new ParseResult(gda, true, currentStr);
  }

  public void readFields(DataInput in) throws IOException {
    bodyType = InferredType.readType(in);
  }
  public void write(DataOutput out) throws IOException {
    out.write(ARRAY_TYPE);
    bodyType.write(out);
  }
}

class UnionType extends InferredType {
  List<InferredType> unionTypes;
  static int unionCounter = 0;

  public UnionType() {
  }
  public UnionType(List<InferredType> unionTypes) {
    this.unionTypes = unionTypes;
  }
  public InferredType hoistUnions() {
    List<InferredType> newUnionTypes = new ArrayList<InferredType>();
    for (InferredType it: unionTypes) {
      if (it instanceof UnionType) {
        UnionType subUnion = (UnionType) it;
        for (InferredType it2: subUnion.unionTypes) {
          newUnionTypes.add(it2.hoistUnions());
        }
      } else {
        newUnionTypes.add(it.hoistUnions());
      }
    }
    return new UnionType(newUnionTypes);
  }
  public Schema getAvroSchema() {
    HashSet<String> observedSchemas = new HashSet<String>();
    List<Schema> fields = new ArrayList<Schema>();
    for (InferredType it: unionTypes) {
      Schema itS = it.getAvroSchema();
      if (itS == null) {
        continue;
      }
      String schemaDesc = itS.toString();
      if (! observedSchemas.contains(schemaDesc)) {
        observedSchemas.add(schemaDesc);
        fields.add(it.getAvroSchema());
      }
    }

    Schema s = Schema.createUnion(fields);
    return s;
  }
  /**
   * Parse the given string, return resulting data if appropriate.
   */
  ParseResult internalParse(String s) {
    String currentStr = s;

    for (InferredType subelt: unionTypes) {
      ParseResult pr = subelt.internalParse(currentStr);
      if (pr != null) {
        return new ParseResult(pr.getData(), pr.hasData(), pr.getRemainingString());
      }
    }
    return null;
  }
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("Union: (");
    for (InferredType it: unionTypes) {
      buf.append(it.toString() + ", ");
    }
    buf.append(") ");
    return buf.toString();
  }
  public double getDescriptionCost() {
    double dc = CARD_COST;
    for (InferredType it: unionTypes) {
      dc += it.getDescriptionCost();
    }
    return dc;
  }
  String createName() {
    return "union-" + unionCounter++;
  }
  public void readFields(DataInput in) throws IOException {
    int numUnionElts = in.readInt();
    for (int i = 0; i < numUnionElts; i++) {
      unionTypes.add(InferredType.readType(in));
    }
  }
  public void write(DataOutput out) throws IOException {
    out.write(UNION_TYPE);
    unionTypes = new ArrayList<InferredType>();
    out.writeInt(unionTypes.size());
    for (InferredType it: unionTypes) {
      it.write(out);
    }
  }
}



