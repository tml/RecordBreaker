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
