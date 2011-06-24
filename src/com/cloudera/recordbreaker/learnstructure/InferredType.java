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

  static int BASE_NOOP = 1;

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
    //
    // Try the naive parse
    //
    ParseResult pr = internalParse(str, null, true);
    if (pr != null && pr.hasData()) {
      return (GenericContainer) pr.getData();
    }

    //
    // Otherwise, we need to consider other union-options.
    // Unfold the candidate decisions into a series of target decisions
    //
    Map<String, Set<Integer>> candidateUnionDecisions = findCandidateUnionDecisions();

    List<HashMap<String, Integer>> allUnionDecisions = new ArrayList<HashMap<String, Integer>>();
    for (Map.Entry<String, Set<Integer>> pair: candidateUnionDecisions.entrySet()) {
      String k = pair.getKey();
      Set<Integer> indices = pair.getValue();

      if (allUnionDecisions.size() == 0) {
        for (Integer index: indices) {
          HashMap<String, Integer> newMap = new HashMap<String, Integer>();
          newMap.put(k, index);
          allUnionDecisions.add(newMap);
        }
      } else {
        List<HashMap<String, Integer>> newUnionDecisions = new ArrayList<HashMap<String, Integer>>();
        for (HashMap<String, Integer> curUnionDecisions: allUnionDecisions) {
          for (Integer index: indices) {
            HashMap<String, Integer> newMap = (HashMap<String, Integer>) curUnionDecisions.clone();
            newMap.put(k, index);
            newUnionDecisions.add(newMap);
          }
        }
        allUnionDecisions = newUnionDecisions;
      }
    }

    //
    // Now execute all possible union decisions
    //
    for (Map<String, Integer> targetUnionDecisions: allUnionDecisions) {
      pr = internalParse(str, targetUnionDecisions, true);
      if (pr != null && pr.hasData()) {
        return (GenericContainer) pr.getData();
      }
    }
    //System.err.println("Blargh!!  Failed on " + str);
    return null;
  }
  abstract ParseResult internalParse(String s, Map<String, Integer> targetUnionDecisions, boolean mustConsumeStr);
  abstract Map<String, Set<Integer>> findCandidateUnionDecisions();
  abstract List<String> getBases();

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
  public abstract List<InferredType> materializeWithoutUnions();
  abstract InferredType duplicate();
  public String getDocString() {
    return "";
  }
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
  int tokenClassIdentifier;
  String tokenParameter;
  Schema schema = null;
  List<String> sampleStrs = null;

  static int fieldCounter = 0;
  public BaseType() {
  }
  public BaseType(Token.AbstractToken token, List<String> sampleStrs) {
    this.sampleStrs = sampleStrs;
    this.tokenClassIdentifier = token.getClassId();
    this.tokenParameter = token.getParameter();
    //System.err.println("Token parameter: " + tokenParameter);
    this.schema = computeAvroSchema();
  }
  public BaseType(int tokenClassIdentifier, List<String> sampleStrs, String tokenParameter) {
    this.sampleStrs = sampleStrs;
    this.tokenClassIdentifier = tokenClassIdentifier;
    this.tokenParameter = tokenParameter;
    //System.err.println("Token parameter: " + tokenParameter);
    this.schema = computeAvroSchema();
  }
  public InferredType hoistUnions() {
    return this;
  }
  public List<InferredType> materializeWithoutUnions() {
    List<InferredType> toReturn = new ArrayList<InferredType>();
    toReturn.add(this.duplicate());
    return toReturn;
  }
  InferredType duplicate() {
    return new BaseType(tokenClassIdentifier, sampleStrs, tokenParameter);
  }

  Schema computeAvroSchema() {
    return Token.AbstractToken.createAvroSchema(tokenClassIdentifier, tokenParameter, name);
  }
  public Schema getAvroSchema() {
    return schema;
  }
  public String getDocString() {
    StringBuffer buf = new StringBuffer();
    buf.append("Example data: ");
    for (Iterator<String> it = sampleStrs.iterator(); it.hasNext(); ) {
      String tokStr = it.next();
      buf.append("'" + tokStr + "'");
      if (it.hasNext()) {
        buf.append(", ");
      }
    }
    return buf.toString();
  }
  ParseResult internalParse(String s, Map<String, Integer> targetUnionDecisions, boolean mustConsumeStr) {
    List<Token.AbstractToken> outputToks = new ArrayList<Token.AbstractToken>();
    String newStr = Tokenizer.attemptParse(tokenClassIdentifier, tokenParameter, s, outputToks);
    if (newStr == null || (mustConsumeStr && newStr.trim().length()!=0)) {
      return null;
    }
    assert(outputToks.size()==1);
    // outputToks should contain just one result.
    return new ParseResult(outputToks.get(0).get(), Token.AbstractToken.hasData(tokenClassIdentifier), newStr);
  }
  Map<String, Set<Integer>> findCandidateUnionDecisions() {
    return new HashMap<String, Set<Integer>>();
  }
  List<String> getBases() {
    List<String> tr = new ArrayList<String>();
    tr.add(toString());
    return tr;
  }
  public String toString() {
    return "Base: " + Token.AbstractToken.getStrDesc(tokenClassIdentifier, tokenParameter) + " ";
  }
  public double getDescriptionCost() {
    return CARD_COST;
  }
  String createName() {
    return "base_" + fieldCounter++;
  }
  public void readFields(DataInput in) throws IOException {
    // instance-specific
    this.sampleStrs = new ArrayList<String>();
    int numSamples = in.readInt();
    for (int i = 0; i < numSamples; i++) {
      sampleStrs.add(UTF8.readString(in).toString());
    }
    this.tokenClassIdentifier = in.readInt();
    if (in.readBoolean()) {
      this.tokenParameter = UTF8.readString(in);
    } else {
      this.tokenParameter = null;
    }
    this.schema = computeAvroSchema();
  }
  public void write(DataOutput out) throws IOException {
    out.writeInt(sampleStrs.size());
    for (int i = 0; i < sampleStrs.size(); i++) {
      UTF8.writeString(out, sampleStrs.get(i));
    }
    out.writeInt(tokenClassIdentifier);
    out.writeBoolean(tokenParameter != null);
    if (tokenParameter != null) {
      UTF8.writeString(out, tokenParameter);
    }
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
  Schema schema;

  public StructType() {
  }
  public StructType(List<InferredType> structTypes) {
    this.structTypes = structTypes;
    this.schema = computeAvroSchema();
  }
  void addElt(InferredType structElt) {
    this.structTypes.add(structElt);
  }
  public InferredType hoistUnions() {
    List<InferredType> newStructTypes = new ArrayList<InferredType>();
    for (InferredType it: structTypes) {
      newStructTypes.add(it.hoistUnions());
    }
    return new StructType(newStructTypes);
  }
  public List<InferredType> materializeWithoutUnions() {
    List<InferredType> newStructs = new ArrayList<InferredType>();

    for (int i = 0; i < structTypes.size(); i++) {
      List<InferredType> curTrees = structTypes.get(i).materializeWithoutUnions();

      if (i == 0) {
        for (int j = 0; j < curTrees.size(); j++) {
          List<InferredType> curTypeList = new ArrayList<InferredType>();
          curTypeList.add(curTrees.get(j));
          newStructs.add(new StructType(curTypeList));
        }
      } else {
        List<InferredType> evenNewerStructs = new ArrayList<InferredType>();
        evenNewerStructs.addAll(newStructs);
        for (int j = 1; j < curTrees.size(); j++) {         
          for (int k = 0; k < newStructs.size(); k++) {
            evenNewerStructs.add(newStructs.get(k).duplicate());
          }
        }
        for (int j = 0; j < curTrees.size(); j++) {
          for (int k = 0; k < evenNewerStructs.size(); k++) {
            ((StructType) evenNewerStructs.get(k)).addElt(curTrees.get(j));
          }
        }
        newStructs = evenNewerStructs;
      }
    }
    return newStructs;
  }
  InferredType duplicate() {
    List<InferredType> newElts = new ArrayList<InferredType>();
    for (InferredType elt: structTypes) {
      newElts.add(elt.duplicate());
    }
    return new StructType(newElts);
  }

  public Schema getAvroSchema() {
    return schema;
  }
  Schema computeAvroSchema() {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (InferredType it: structTypes) {
      Schema itS = it.getAvroSchema();
      if (itS == null) {
        continue;
      }
      fields.add(new Schema.Field(it.getName(), it.getAvroSchema(), it.getDocString(), it.getDefaultValue()));
    }
    Schema s = Schema.createRecord(name, "RECORD", "", false);
    s.setFields(fields);
    return s;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("(Struct: ");
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
    return "record_" + recordCounter++;
  }
  public void readFields(DataInput in) throws IOException {
    int numStructTypes = in.readInt();
    structTypes = new ArrayList<InferredType>();
    for (int i = 0; i < numStructTypes; i++) {
      structTypes.add(InferredType.readType(in));
    }
    this.schema = computeAvroSchema();
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
  ParseResult internalParse(String s, Map<String, Integer> targetUnionDecisions, boolean mustConsumeStr) {
    boolean hasData = false;
    GenericData.Record gdr = new GenericData.Record(getAvroSchema());
    String currentStr = s;

    for (InferredType subelt: structTypes) {
      if (currentStr.length() == 0) {
        return null;
      }
      ParseResult pr = subelt.internalParse(currentStr, targetUnionDecisions, false);
      if (pr == null) {
        return null;
      }
      if (pr.hasData()) {
        hasData = true;
        gdr.put(subelt.getName(), pr.getData());
      }
      currentStr = pr.getRemainingString();
    }
    if (mustConsumeStr && currentStr.trim().length() != 0) {
      return null;
    }
    return new ParseResult(gdr, hasData, currentStr);
  }
  Map<String, Set<Integer>> findCandidateUnionDecisions() {
    Map<String, Set<Integer>> candidateUnionDecisions = new HashMap<String, Set<Integer>>();
    for (InferredType subelt: structTypes) {
      candidateUnionDecisions.putAll(subelt.findCandidateUnionDecisions());
    }
    return candidateUnionDecisions;
  }
  List<String> getBases() {
    List<String> tr = new ArrayList<String>();
    for (InferredType subelt: structTypes) {
      tr.addAll(subelt.getBases());
    }
    return tr;
  }
}

class ArrayType extends InferredType {
  InferredType bodyType;
  static int arrayCounter = 0;
  Schema schema = null;

  public ArrayType() {
  }
  public ArrayType(InferredType bodyType) {
    this.bodyType = bodyType;
    this.schema = computeAvroSchema();
  }
  public InferredType hoistUnions() {
    return new ArrayType(bodyType.hoistUnions());
  }
  public List<InferredType> materializeWithoutUnions() {
    List<InferredType> newArrays = new ArrayList<InferredType>();
    for (InferredType subtype: bodyType.materializeWithoutUnions()) {
      newArrays.add(new ArrayType(subtype));
    }
    return newArrays;
  }
  InferredType duplicate() {
    return new ArrayType(bodyType);
  }
  public Schema getAvroSchema() {
    return schema;
  }
  Schema computeAvroSchema() {
    return Schema.createArray(bodyType.getAvroSchema());
  }
  public String toString() {
    return "(Array: " + bodyType.toString() + ") ";
  }
  public double getDescriptionCost() {
    return CARD_COST + bodyType.getDescriptionCost();
  }
  String createName() {
    return "array_" + arrayCounter++;
  }
  /**
   * Parse the given string, return resulting data if appropriate.
   */
  ParseResult internalParse(String s, Map<String, Integer> targetUnionDecisions, boolean mustConsumeStr) {
    boolean hasData = false;
    Schema localSchema = getAvroSchema();
    GenericData.Array gda = new GenericData.Array(5, localSchema);
    Map<String, Integer> curUnionDecisions = new HashMap<String, Integer>();
    String currentStr = s;

    while (true) {
      ParseResult pr = bodyType.internalParse(s, targetUnionDecisions, false);
      if (pr == null) {
        break;
      }
      assert(pr.hasData());

      gda.add(pr.getData());
      currentStr = pr.getRemainingString();
    }
    if (mustConsumeStr && currentStr.trim().length() != 0) {
      return null;
    }
    return new ParseResult(gda, true, currentStr);
  }
  Map<String, Set<Integer>> findCandidateUnionDecisions() {
    return bodyType.findCandidateUnionDecisions();
  }
  List<String> getBases() {
    return bodyType.getBases();
  }

  public void readFields(DataInput in) throws IOException {
    bodyType = InferredType.readType(in);
    this.schema = computeAvroSchema();
  }
  public void write(DataOutput out) throws IOException {
    out.write(ARRAY_TYPE);
    bodyType.write(out);
  }
}

class UnionType extends InferredType {
  List<InferredType> unionTypes;
  static int unionCounter = 0;
  Schema schema = null;

  public UnionType() {
  }
  public UnionType(List<InferredType> unionTypes) {
    this.unionTypes = unionTypes;
    this.schema = computeAvroSchema();
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

  public List<InferredType> materializeWithoutUnions() {
    List<InferredType> allOptions = new ArrayList<InferredType>();
    for (InferredType branch: unionTypes) {
      allOptions.addAll(branch.materializeWithoutUnions());
    }
    return allOptions;
  }

  InferredType duplicate() {
    List<InferredType> newBranches = new ArrayList<InferredType>();
    for (InferredType branch: unionTypes) {
      newBranches.add(branch.duplicate());
    }
    return new UnionType(newBranches);
  }

  public Schema getAvroSchema() {
    return schema;
  }
  Schema computeAvroSchema() {
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
    return Schema.createUnion(fields);
  }
  /**
   * Parse the given string, return resulting data if appropriate.
   */
  ParseResult internalParse(String s, Map<String, Integer> targetUnionDecisions, boolean mustConsumeStr) {
    //
    // If there's no target decision, then go ahead and try all branches.
    //
    if (targetUnionDecisions == null || targetUnionDecisions.get(name) == null) {
      for (InferredType subelt: unionTypes) {
        ParseResult pr = subelt.internalParse(s, targetUnionDecisions, false);
        if (pr != null && (!mustConsumeStr || (mustConsumeStr && pr.getRemainingString().trim().length() == 0))) {
          return new ParseResult(pr.getData(), pr.hasData(), pr.getRemainingString());
        }
      }
      return null;
    }

    //
    // If there is a target decision, then carry it out.
    //
    InferredType subelt = unionTypes.get(targetUnionDecisions.get(name));
    ParseResult pr = subelt.internalParse(s, targetUnionDecisions, false);
    if (pr != null && (!mustConsumeStr || (mustConsumeStr && pr.getRemainingString().trim().length() == 0))) {
      return new ParseResult(pr.getData(), pr.hasData(), pr.getRemainingString());
    }
    return null;
  }

  /**
   */
  boolean isPrefixOf(List<String> a, List<String> b) {
    for (int i = 0; i < a.size(); i++) {
      if (i >= b.size() || a.get(i).compareTo(b.get(i)) != 0) {
        return false;
      }
    }
    return true;
  }
 
  /**
   */
  Map<String, Set<Integer>> findCandidateUnionDecisions() {
    HashSet<Integer> curDecisions = new HashSet<Integer>();
    for (int i = 0; i < unionTypes.size(); i++) {
      for (int j = i+1; j < unionTypes.size(); j++) {
        List<String> iBases = unionTypes.get(i).getBases();
        List<String> jBases = unionTypes.get(j).getBases();
        if (isPrefixOf(iBases, jBases) || isPrefixOf(jBases, iBases)) {
          curDecisions.add(i);
          curDecisions.add(j);
        }
      }
    }
    Map<String, Set<Integer>> candidateUnionDecisions = new HashMap<String, Set<Integer>>();
    for (InferredType subelt: unionTypes) {
      candidateUnionDecisions.putAll(subelt.findCandidateUnionDecisions());
    }
    if (curDecisions.size() > 0) {
      candidateUnionDecisions.put(name, curDecisions);
    }
    return candidateUnionDecisions;
  }

  /**
   */
  List<String> getBases() {
    // We stop the base-evaluation when we hit a union.
    return new ArrayList<String>();
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("(Union (" + unionTypes.size() + "): ");
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
    return "union_" + unionCounter++;
  }
  public void readFields(DataInput in) throws IOException {
    int numUnionElts = in.readInt();
    this.unionTypes = new ArrayList<InferredType>();
    for (int i = 0; i < numUnionElts; i++) {
      unionTypes.add(InferredType.readType(in));
    }
    this.schema = computeAvroSchema();
  }
  public void write(DataOutput out) throws IOException {
    out.write(UNION_TYPE);
    out.writeInt(unionTypes.size());
    for (InferredType it: unionTypes) {
      it.write(out);
    }
  }
}



