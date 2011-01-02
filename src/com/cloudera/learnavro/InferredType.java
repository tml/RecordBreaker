// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.lang.reflect.*;
import java.util.*;

/*********************************************************
 * InferredType is returned by TypeInference.infer() for the inferred record type
 * of a file's contents.  It has several subclasses.
 *********************************************************/
public class InferredType {
}

class BaseType extends InferredType {
  Class baseTokenClass;
  public BaseType(Class baseTokenClass) {
    this.baseTokenClass = baseTokenClass;
  }
  public String toString() {
    String baseTokenIdentifier = null;
    try {
      baseTokenIdentifier = (String) baseTokenClass.getField("tokenTypeLabel").get(null);
    } catch (NoSuchFieldException nsfe) {
      baseTokenIdentifier = baseTokenClass.toString();
      nsfe.printStackTrace();
    } catch (IllegalAccessException iae) {
      baseTokenIdentifier = baseTokenClass.toString();
      iae.printStackTrace();
    }
    return "Base: " + baseTokenIdentifier + " ";
  }
}

class StructType extends InferredType {
  List<InferredType> structTypes;
  public StructType(List<InferredType> structTypes) {
    this.structTypes = structTypes;
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
}

class ArrayType extends InferredType {
  InferredType bodyType;
  public ArrayType(InferredType bodyType) {
    this.bodyType = bodyType;
  }
  public String toString() {
    return "Array: (" + bodyType.toString() + ") ";
  }
}

class UnionType extends InferredType {
  List<InferredType> unionTypes;
  public UnionType(List<InferredType> unionTypes) {
    this.unionTypes = unionTypes;
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
}



