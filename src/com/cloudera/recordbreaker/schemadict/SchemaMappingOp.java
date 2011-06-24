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

/***************************************************
 * SchemaMappingOp describes a schema-modification step.
 *
 * @author mjc
 ***************************************************/
public class SchemaMappingOp { 
  public static int CREATE_OP = 1;
  public static int DELETE_OP = 2;
  public static int TRANSFORM_OP = 3;
  public static int NEGATIVE_TRANSFORM_OP = 4;

  int opcode;
  SchemaStatisticalSummary s1;
  SchemaStatisticalSummary s2;
  int nodeid1;
  int nodeid2;

  /**
   */
  public SchemaMappingOp() {
  }
  public SchemaMappingOp(int opcode, SchemaStatisticalSummary s, int nodeid) {
    this.opcode = opcode;
    this.s1 = s;
    this.nodeid1 = nodeid;
  }
  public SchemaMappingOp(int opcode, SchemaStatisticalSummary s1, int nodeid1, SchemaStatisticalSummary s2, int nodeid2) {
    this.opcode = opcode;
    this.s1 = s1;
    this.nodeid1 = nodeid1;
    this.s2 = s2;
    this.nodeid2 = nodeid2;
  }
  public String getS1DatasetLabel() {
    return s1.getDatasetLabel();
  }
  public String getS1FieldLabel() {
    return s1.getLabel(nodeid1);
  }
  public String getS1FieldType() {
    return s1.getTypeDesc(nodeid1);
  }
  public String getS1DocStr() {
    return s1.getDocStr(nodeid1);
  }
  public String getS2DatasetLabel() {
    return s2.getDatasetLabel();
  }
  public String getS2FieldLabel() {
    return s2.getLabel(nodeid2);
  }
  public String getS2FieldType() {
    return s2.getTypeDesc(nodeid2);
  }
  public String getS2DocStr() {
    return s2.getDocStr(nodeid2);
  }
  public int getOpcode() {
    return opcode;
  }
  public int getNodeId1() {
    return nodeid1;
  }
  public int getNodeId2() {
    return nodeid2;
  }
  
  public String toString() {
    if (opcode == CREATE_OP) {
      return "CREATE " + nodeid1 + "(" + s1.getDesc(nodeid1) + ")";
    } else if (opcode == DELETE_OP) {
      return "DELETE " + nodeid1 + "(" + s1.getDesc(nodeid1) + ")";
    } else if (opcode == TRANSFORM_OP) {
      return "TRANSFORM " + nodeid1 + " (" + s1.getDesc(nodeid1) + "," + nodeid1 + ")" + " => " + nodeid2 + " (" + s2.getDesc(nodeid2) + "," + nodeid2 + ")";
    } else if (opcode == NEGATIVE_TRANSFORM_OP) {
      return "NEGATIVE-TRANSFORM " + nodeid1 + " (" + s1.getDesc(nodeid1) + "," + nodeid1 + ")" + " => " + nodeid2 + " (" + s2.getDesc(nodeid2) + "," + nodeid2 + ")";
    } else {
      return "<unknown>";
    }
  }
}