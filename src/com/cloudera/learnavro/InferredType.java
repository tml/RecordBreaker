// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

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
}

class StructType extends InferredType {
  List<InferredType> structTypes;
  public StructType(List<InferredType> structTypes) {
    this.structTypes = structTypes;
  }
}

class ArrayType extends InferredType {
  InferredType bodyType;
  public ArrayType(InferredType bodyType) {
    this.bodyType = bodyType;
  }
}

class UnionType extends InferredType {
  List<InferredType> unionTypes;
  public UnionType(List<InferredType> unionTypes) {
    this.unionTypes = unionTypes;
  }
}



