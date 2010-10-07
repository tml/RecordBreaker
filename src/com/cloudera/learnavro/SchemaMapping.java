// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.util.List;

/*****************************************************
 * SchemaMapping describes a mapping between the labels of one schema
 * to those of another.  A mapping consists of a series of pairs of 
 * the form (schemaARef, schemaBRef).  A "ref" is a dotted attribute
 * reference.  Such a pair indicates that the attribute or structure referred 
 * to by "schemaARef" in schema A is very similar to the attr/struct referred
 * to by "schemaBRef" in schema B.
 *
 * Some attributes in each schema may not be present in the mapping.  Any attribute
 * that is present should only be present once.
 *
 * @author mjc
 ******************************************************/
public class SchemaMapping { 
  SchemaStatisticalSummary s1;
  SchemaStatisticalSummary s2;
  double dist;
  List<SchemaMappingOp> mapping;

  /**
   * A computed SchemaMapping has pointers to two schema summaries, plus a distance and a mapping between them.
   */
  public SchemaMapping(SchemaStatisticalSummary s1, SchemaStatisticalSummary s2, double dist, List<SchemaMappingOp> mapping) {
    this.s1 = s1;
    this.s2 = s2;
    this.dist = dist;
    this.mapping = mapping;
  }


  //////////////////////////////////////////////
  // Accessors
  //////////////////////////////////////////////
  public SchemaStatisticalSummary getS1() {
    return s1;
  }
  public SchemaStatisticalSummary getS2() {
    return s2;
  }
  public double getDist() {
    return dist;
  }
  public List<SchemaMappingOp> getMapping() {
    return mapping;
  }

  /**
   */
  public String toString() {
    return "<mapping>";
  }
}
