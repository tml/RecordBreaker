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

/*****************************************************
 * DictionaryMapping is a SchemaMapping that includes a SchemaDictionary item.
 *
 * @author mjc
 ******************************************************/
public class DictionaryMapping implements Comparable {
  SchemaMapping mapping;
  SchemaDictionaryEntry dictElt;

  public DictionaryMapping(SchemaMapping mapping, SchemaDictionaryEntry dictElt) {
    this.mapping = mapping;
    this.dictElt = dictElt;
  }
  public SchemaMapping getMapping() {
    return mapping;
  }
  public SchemaDictionaryEntry getDictEntry() {
    return dictElt;
  }
  public int compareTo(Object o) {
    DictionaryMapping dm = (DictionaryMapping) o;
    double thisDist = this.mapping.getDist();
    double dmDist = dm.getMapping().getDist();
    if (thisDist < dmDist) {
      return -1;
    } else if (thisDist > dmDist) {
      return 1;
    } else {
      return this.dictElt.getSchema().toString().compareTo(dm.getDictEntry().getSchema().toString());
    }
  }
  public String toString() {
    return "Map " + dictElt;
  }
}

