// (c) Copyright (2010) Cloudera, Inc.
package com.cloudera.learnavro;

import java.io.*;
import java.util.*;

/**********************************************************
 * TypeInference takes a set of parsed tokens and transforms them
 * into a parsed record format.
 *
 **********************************************************/
public class TypeInference {
  /////////////////////////////////////
  // Inner Classes
  /////////////////////////////////////
  static class TypeProphecy {
  }

  static class BaseProphecy extends TypeProphecy {
    Class baseType;
    public BaseProphecy(Class baseType) {
      this.baseType = baseType;
    }
    Class getBaseType() {
      return baseType;
    }
  }

  static class StructProphecy extends TypeProphecy {
    List<List<List<Token.AbstractToken>>> structElts;
    public StructProphecy(List<List<List<Token.AbstractToken>>> structElts) {
      this.structElts = structElts;
    }
    public List<List<List<Token.AbstractToken>>> getStructElts() {
      return structElts;
    }
  }

  static class ArrayProphecy extends TypeProphecy {
    List<List<Token.AbstractToken>> first;
    List<List<Token.AbstractToken>> body;
    List<List<Token.AbstractToken>> last;

    public ArrayProphecy(List<List<Token.AbstractToken>> first, List<List<Token.AbstractToken>> body, List<List<Token.AbstractToken>> last) {
      this.first = first;
      this.body = body;
      this.last = last;
    }
    public List<List<Token.AbstractToken>> getFirst() {
      return first;
    }
    public List<List<Token.AbstractToken>> getBody() {
      return body;
    }
    public List<List<Token.AbstractToken>> getLast() {
      return last;
    }
  }

  static class UnionProphecy extends TypeProphecy {
    List<List<List<Token.AbstractToken>>> unionElts;
    public UnionProphecy(List<List<List<Token.AbstractToken>>> unionElts) {
      this.unionElts = unionElts;
    }
    public List<List<List<Token.AbstractToken>>> getUnionElements() {
      return unionElts;
    }
  }

  /////////////////////////////////////
  // Public methods and support methods
  /////////////////////////////////////
  public TypeInference() {
  }

  /**
   * Accepts a list of chunks, each of which is a list of Tokens.
   * It uses patterns of token appearance to discern the record structure.
   * This is the core of the LearnPADS algorithm.
   */
  public static InferredType infer(List<List<Token.AbstractToken>> chunks) {
    InferredType typeTree = discover(chunks);
    //typeTree.ensureParsability();
    return typeTree;
  }

  /**
   * The oracle() function attempts to predict the best type for the data given by 'chunks'.
   */
  private static TypeProphecy oracle(List<List<Token.AbstractToken>> chunks) {
    //////////////////////////////////////////////////////////////
    // Phase 1: Handling chunks that appear homogenous (at this meta-level, at least)
    //////////////////////////////////////////////////////////////
    // Start by gathering some stats on the chunks
    HashSet<String> uniqTokTypes = new HashSet<String>();
    int noops = 0;
    int numToks = 0;
    for (List<Token.AbstractToken> chunk: chunks) {
      if (chunk.size() == 0) {
        noops++;
      }
      for (Token.AbstractToken tok: chunk) {
        uniqTokTypes.add(tok.getId());
        numToks++;
      }
    }

    //
    // CONDITION: Is the chunkset empty?
    //
    if (numToks == 0 && noops == chunks.size()) {
      return new BaseProphecy(Token.NoopToken.class);
    }
    //
    // CONDITION: Does the chunkset consist of a single column of one type of token?
    //
    if (numToks == chunks.size() && uniqTokTypes.size() == 1) {
      // If so, grab an example from the column
      Token.AbstractToken prizeToken = chunks.get(0).get(0);
      if (! (prizeToken instanceof Token.MetaToken)) {
        // If it's not a MetaToken, then it's easy: we prophesy a data column consisting of a single basic type
        return new BaseProphecy(prizeToken.getClass());
      } else {
        //
        // If it IS a metatoken, then we prophesy a struct that contains what's inside the metatoken.
        // In other words, we break apart the pair-delimited "meta" structure in this step.
        //
        // Note: the getId() fn of MetaToken returns the char-delimiter, so if we've got just one elt in uniqTokTypes,
        // it implies that there's a single MetaToken type in the entire chunkset.
        //
        Token.MetaToken mtok = (Token.MetaToken) prizeToken;

        List<List<Token.AbstractToken>> startChunkList = new ArrayList<List<Token.AbstractToken>>();
        List<Token.AbstractToken> startChunk = new ArrayList<Token.AbstractToken>();
        startChunk.add(mtok.getStartToken());
        startChunkList.add(startChunk);

        List<List<Token.AbstractToken>> middleChunkList = new ArrayList<List<Token.AbstractToken>>();
        for (List<Token.AbstractToken> chunk: chunks) {
          middleChunkList.add(((Token.MetaToken) chunk.get(0)).getMiddleChunk());
        }

        List<List<Token.AbstractToken>> endChunkList = new ArrayList<List<Token.AbstractToken>>();
        List<Token.AbstractToken> endChunk = new ArrayList<Token.AbstractToken>();
        endChunk.add(mtok.getEndToken());
        endChunkList.add(endChunk);

        List<List<List<Token.AbstractToken>>> structElts = new ArrayList<List<List<Token.AbstractToken>>>();
        structElts.add(startChunkList);
        structElts.add(middleChunkList);
        structElts.add(endChunkList);
        return new StructProphecy(structElts);
      }
    }

    //////////////////////////////////////////////////////////////
    // Phase 2: Handling heterogeneous chunks
    //////////////////////////////////////////////////////////////
    // As usual, start by gathering some statistics
    List<Histogram> normalizedHistograms = Histogram.computeNormalizedHistograms(chunks);
    List<List<Histogram>> clusteredHistograms = Histogram.clusterHistograms(normalizedHistograms);

    int clusterId = 0;
    System.err.println("There are " + normalizedHistograms.size() + " unique normalized histograms.");
    System.err.println("There are " + clusteredHistograms.size() + " histogram clusters.");
    System.err.println();
    for (List<Histogram> cluster: clusteredHistograms) {
      System.err.println("Cluster " + clusterId + " has " + cluster.size() + " elements.");
      int j = 0;
      for (Histogram h: cluster) {
        System.err.println("  " + j + ".  " + h);
        j++;
      }
      System.err.println();
      clusterId++;
    }

    //
    // CONDITION: 
    //
    return null;
  }

  /**
   */
  private static InferredType discover(List<List<Token.AbstractToken>> chunks) {
    // Error testing
    assert(chunks.size() > 0);

    //
    // Type predictions from the oracle come in one of four flavors: Base, Struct, Array, or Union
    //
    TypeProphecy typePrediction = oracle(chunks);
    if (typePrediction instanceof BaseProphecy) {
      return new BaseType(((BaseProphecy) typePrediction).getBaseType());

    } else if (typePrediction instanceof StructProphecy) {
      StructProphecy sp = (StructProphecy) typePrediction;
      List<InferredType> structDataTypes = new ArrayList<InferredType>();
      for (List<List<Token.AbstractToken>> structElt: sp.getStructElts()) {
        structDataTypes.add(discover(structElt));
      }
      return new StructType(structDataTypes);

    } else if (typePrediction instanceof ArrayProphecy) {
      ArrayProphecy ap = (ArrayProphecy) typePrediction;
      assert(ap.getBody().size() > 0);

      List<InferredType> structDataTypes = new ArrayList<InferredType>();
      if (ap.getFirst().size() > 0) {
        structDataTypes.add(discover(ap.getFirst()));
      }
      structDataTypes.add(new ArrayType(discover(ap.getBody())));
      if (ap.getLast().size() > 0) {
        structDataTypes.add(discover(ap.getLast()));
      }
      return new StructType(structDataTypes);

    } else if (typePrediction instanceof UnionProphecy) {
      UnionProphecy up = (UnionProphecy) typePrediction;
      List<InferredType> unionDataTypes = new ArrayList<InferredType>();
      
      for (List<List<Token.AbstractToken>> unionElt: up.getUnionElements()) {
        unionDataTypes.add(discover(unionElt));
      }
      return new UnionType(unionDataTypes);
    }
    return null;
  }
}
