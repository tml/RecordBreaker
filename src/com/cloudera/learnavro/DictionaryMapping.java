// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

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

