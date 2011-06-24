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

import java.io.*;
import java.util.*;

/**********************************************************
 * TypeInference takes a set of parsed tokens and transforms them
 * into a parsed record format.
 *
 **********************************************************/
public class TypeInference {
  static int MAX_SAMPLES = 3;

  /////////////////////////////////////
  // Inner Classes
  /////////////////////////////////////
  static class TypeProphecy {
  }

  static class BaseProphecy extends TypeProphecy {
    Token.AbstractToken token;
    List<Token.AbstractToken> samples;
    public BaseProphecy(Token.AbstractToken token, List<Token.AbstractToken> samples) {
      this.token = token;
      this.samples = samples;
    }
    public Token.AbstractToken getToken() {
      return token;
    }
    public List<Token.AbstractToken> getSamples() {
      return samples;
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
    typeTree = typeTree.hoistUnions();
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
      //System.err.println("BASE-1");
      List<Token.AbstractToken> samples = new ArrayList<Token.AbstractToken>();
      Token.NoopToken noop = new Token.NoopToken();
      samples.add(noop);
      return new BaseProphecy(noop, samples);
    }
    //
    // CONDITION: Does the chunkset consist of a single column of one type of token?
    //
    if (numToks == chunks.size() && uniqTokTypes.size() == 1) {
      // If so, grab an example from the column
      Token.AbstractToken prizeToken = chunks.get(0).get(0);
      if (! (prizeToken instanceof Token.MetaToken)) {
        // If it's not a MetaToken, then it's easy: we prophesy a data column consisting of a single basic type
        List<Token.AbstractToken> samples = new ArrayList<Token.AbstractToken>();
        int numSamples = 0;
        for (List<Token.AbstractToken> curChunk: chunks) {
          samples.add(curChunk.get(0));
          numSamples++;
          if (numSamples >= MAX_SAMPLES) {
            break;
          }
        }
        return new BaseProphecy(prizeToken, samples);
      } else {
        //System.err.println("STRUCT-1");
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

    //
    // CONDITION: Does a Struct exist?
    //
    SortedSet<List<Histogram>> structHistogramClusters = new TreeSet<List<Histogram>>(new Comparator() {
      public int compare(Object o1, Object o2) {
        List<Histogram> cluster1 = (List<Histogram>) o1;
        List<Histogram> cluster2 = (List<Histogram>) o2;

        double minResidualMass1 = Double.MAX_VALUE;
        double minResidualMass2 = Double.MAX_VALUE;
        for (Histogram h: cluster1) {
          minResidualMass1 = Math.min(minResidualMass1, h.getResidualMass());
        }
        for (Histogram h: cluster2) {
          minResidualMass2 = Math.min(minResidualMass2, h.getResidualMass());
        }
        if (minResidualMass1 < minResidualMass2) {
          return -1;
        } else if (minResidualMass1 > minResidualMass2) {
          return 1;
        } else {
          return o1.hashCode() - o2.hashCode();
        }
      }
    });
    for (List<Histogram> histogramCluster: clusteredHistograms) {
      boolean allHistogramsPass = true;
      for (Histogram h: histogramCluster) {
        if (! h.passStructStatisticalTest(chunks.size())) {
          allHistogramsPass = false;
        }
      }

      if (allHistogramsPass) {
        structHistogramClusters.add(histogramCluster);
      }
    }

    List<Histogram> bestCluster = null;
    if (structHistogramClusters.size() > 0) {
      // Find the best-scoring histogram cluster
      bestCluster = structHistogramClusters.first();
    }
    //
    // REMIND - mjc - is it OK if the bestCluster consists of a single histogram?
    // It has to be, in order for the alg to be correct in some scenarios.  (e.g., when the file 
    // contains many chunks consisting of ";;")
    //
    //
    if (bestCluster != null) {
      // Find the types that are present in that cluster
      Set<String> bestClusterTypes = new HashSet<String>();
      for (Histogram h: bestCluster) {
        bestClusterTypes.add(h.getHistogramType());
      }

      //
      // We now count how many type-profiles in the chunk data are also relevant to the currently-chosen best cluster.
      // A profile lists the token-types.  It tells how many distinct type-orderings and frequencies are in the chunk data.
      // If there is a single profile across all the chunks, then we create a struct.
      // If there are multiple profiles, then we create a union.
      //

      //
      // This is not being computed correctly.  We need to include the inter-identified tokens.
      // Right now, we could have very different inter-identified-token items, and we would consider
      // them all to be identical.
      //
      Set<String> allTypeProfiles = new HashSet<String>();
      for (List<Token.AbstractToken> chunk: chunks) {
        StringBuffer curTypeProfile = new StringBuffer();
        StringBuffer curField = new StringBuffer();
        curField.append("(");
        for (Token.AbstractToken tok: chunk) {
          if (bestClusterTypes.contains(tok.getId())) {
            curField.append(")");
            curTypeProfile.append(curField.toString());
            curTypeProfile.append("_");
            curTypeProfile.append("(" + tok.getId() + ")");
            curTypeProfile.append("_");
            curField = new StringBuffer();
            curField.append("(");
          } else {
            curField.append(tok.getId() + ",");
          }
        }
        curField.append(")");
        curTypeProfile.append(curField.toString());
        allTypeProfiles.add(curTypeProfile.toString());
      }

      //
      // Is it a STRUCT or a UNION?
      //
      if (allTypeProfiles.size() == 1) {
        // It's a STRUCT.
        //System.err.println("STRUCT-2");

        //
        // structChunks is responsible for tracking all the ingredients of the prophesied struct.  
        // There are 2x+1 as many elts in structChunks as there are tokens identified by the target histogram cluster.
        // The 1st field in the prophesied struct has all tokens up (but not including) the 1st identified token.
        // The 2nd field has the 1st identified token.
        // The 3rd field in the prophesied struct has all tokens after the 1st id'd token and up to (but not including) the 2nd
        // The 4th field has the 2nd identified token.
        // ... and so on.  The final field has all the tokens AFTER the final token that appears in the target histogram-cluster
        //
        Map<Integer, List<List<Token.AbstractToken>>> structChunks = new TreeMap<Integer, List<List<Token.AbstractToken>>>();
        // For each chunk in the input...
        for (List<Token.AbstractToken> chunk: chunks) {
          //
          // Add to 'structChunks' as appropriate.
          //
          int lastIdentifiedIndex = -1;
          for (int i = 0; i < chunk.size(); i++) {
            Token.AbstractToken tok = chunk.get(i);
            if (bestClusterTypes.contains(tok.getId())) {
              List<List<Token.AbstractToken>> preludeChunkList = structChunks.get(2 * i);
              if (preludeChunkList == null) {
                preludeChunkList = new ArrayList<List<Token.AbstractToken>>();
                structChunks.put(2*i, preludeChunkList);
              }
              List<Token.AbstractToken> preludeChunk = new ArrayList<Token.AbstractToken>();
              for (int j = lastIdentifiedIndex+1; j < i; j++) {
                preludeChunk.add(chunk.get(j));
              }
              preludeChunkList.add(preludeChunk);

              List<List<Token.AbstractToken>> identifiedChunkList = structChunks.get(2 * i + 1);
              if (identifiedChunkList == null) {
                identifiedChunkList = new ArrayList<List<Token.AbstractToken>>();
                structChunks.put(2 * i + 1, identifiedChunkList);
              }
              List<Token.AbstractToken> identifiedChunk = new ArrayList<Token.AbstractToken>();
              identifiedChunk.add(chunk.get(i));
              identifiedChunkList.add(identifiedChunk);

              lastIdentifiedIndex = i;
            }
          }
          List<List<Token.AbstractToken>> suffixChunkList = structChunks.get(2 * chunk.size() + 1);
          if (suffixChunkList == null) {
            suffixChunkList = new ArrayList<List<Token.AbstractToken>>();
            structChunks.put(2 * chunk.size() + 1, suffixChunkList);
          }
          List<Token.AbstractToken> suffixChunk = new ArrayList<Token.AbstractToken>();
          for (int j = lastIdentifiedIndex+1; j < chunk.size(); j++) {
            suffixChunk.add(chunk.get(j));
          }
          suffixChunkList.add(suffixChunk);
        }

        // Make sure that for each chunkList in 'structChunks', there is at least one non-empty chunk.
        for (Iterator<Integer> it = structChunks.keySet().iterator(); it.hasNext(); ) {
          Integer chunkId = it.next();
          List<List<Token.AbstractToken>> chunkList = structChunks.get(chunkId);
          int numTokensInChunkList = 0;
          for (List<Token.AbstractToken> chunk: chunkList) {
            numTokensInChunkList += chunk.size();
          }
          if (numTokensInChunkList == 0) {
            it.remove();
          }
        }

        // Transform the map into a chunklist
        List<List<List<Token.AbstractToken>>> structChunkList = new ArrayList<List<List<Token.AbstractToken>>>();
        for (Integer chunkId: structChunks.keySet()) {
          List<List<Token.AbstractToken>> chunkList = structChunks.get(chunkId);
          structChunkList.add(chunkList);
        }
        //System.err.println("STRUCT-2");
        return new StructProphecy(structChunkList);
      } else {
        // It's a UNION.
        // A UnionProphecy requires a number of chunklists, one for each type profile
        //System.err.println("UNION-1");
        Map<String, List<List<Token.AbstractToken>>> unionMap = new HashMap<String, List<List<Token.AbstractToken>>>();
        for (List<Token.AbstractToken> chunk: chunks) {
          StringBuffer curTypeProfile = new StringBuffer();
          StringBuffer curField = new StringBuffer();
          curField.append("(");
          for (Token.AbstractToken tok: chunk) {
            if (bestClusterTypes.contains(tok.getId())) {
              curTypeProfile.append(tok.getId() + "_");
              curField.append(")");
              curTypeProfile.append(curField.toString());
              curTypeProfile.append("_");
              curTypeProfile.append("(" + tok.getId() + ")");
              curTypeProfile.append("_");
              curField = new StringBuffer();
              curField.append("(");
            } else {
              curField.append(tok.getId() + ",");
            }
          }
          curField.append(")");
          curTypeProfile.append(curField.toString());

          List<List<Token.AbstractToken>> unionChunks = unionMap.get(curTypeProfile.toString());
          if (unionChunks == null) {
            unionChunks = new ArrayList<List<Token.AbstractToken>>();
            unionMap.put(curTypeProfile.toString(), unionChunks);
          }
          unionChunks.add(chunk);
        }

        List<List<List<Token.AbstractToken>>> unionChunklists = new ArrayList<List<List<Token.AbstractToken>>>();
        for (List<List<Token.AbstractToken>> unionChunklist: unionMap.values()) {
          unionChunklists.add(unionChunklist);
        }
        return new UnionProphecy(unionChunklists);
      }
    } else {
      //
      // It might be an ARRAY....
      //
      SortedSet<List<Histogram>> arrayHistogramClusters = new TreeSet<List<Histogram>>(new Comparator() {
        public int compare(Object o1, Object o2) {
          List<Histogram> cluster1 = (List<Histogram>) o1;
          List<Histogram> cluster2 = (List<Histogram>) o2;

          double maxCoverage1 = 0;
          double maxCoverage2 = 0;

          for (Histogram h: cluster1) {
            maxCoverage1 = Math.max(maxCoverage1, h.getCoverage());
          }
          for (Histogram h: cluster2) {
            maxCoverage2 = Math.max(maxCoverage2, h.getCoverage());
          }

          if (maxCoverage1 < maxCoverage2) {
            return 1;
          } else if (maxCoverage1 > maxCoverage2) {
            return -1;
          } else {
            return o1.hashCode() - o2.hashCode();
          }
        }
      });
      for (List<Histogram> histogramCluster: clusteredHistograms) {
        boolean allHistogramsPass = true;
        for (Histogram h: histogramCluster) {
          if (! h.passArrayStatisticalTest(chunks.size())) {
            allHistogramsPass = false;
          }
        }
        if (allHistogramsPass) {
          arrayHistogramClusters.add(histogramCluster);
        }
      }
      if (arrayHistogramClusters.size() > 0) {
        // It's going to be an array!
        List<Histogram> bestArrayCluster = arrayHistogramClusters.first();
        Set<String> bestClusterTypes = new HashSet<String>();
        for (Histogram h: bestArrayCluster) {
          bestClusterTypes.add(h.getHistogramType());
        }

        //
        // Goal is to split the array into three chunklists: preamble, repeated-body, and post-amble.
        //
        List<List<Token.AbstractToken>> preambles = new ArrayList<List<Token.AbstractToken>>();
        List<List<Token.AbstractToken>> middles = new ArrayList<List<Token.AbstractToken>>();
        List<List<Token.AbstractToken>> postambles = new ArrayList<List<Token.AbstractToken>>();

        //
        // For each chunk of input...
        // 
        for (List<Token.AbstractToken> chunk: chunks) {
          List<Token.AbstractToken> preamble = new ArrayList<Token.AbstractToken>();
          List<List<Token.AbstractToken>> middleGroup = new ArrayList<List<Token.AbstractToken>>();
          List<Token.AbstractToken> curMiddle = new ArrayList<Token.AbstractToken>();
          List<Token.AbstractToken> postamble = new ArrayList<Token.AbstractToken>();
          Set<String> observedClusterTypes = new HashSet<String>();
          int mode = 0;

          for (Token.AbstractToken tok: chunk) {
            // Transit
            if (mode == 0 && observedClusterTypes.size() == bestClusterTypes.size()) {
              mode = 1;
            } else if (mode == 1 && ! bestClusterTypes.contains(tok.getId())) {
              mode = 2;
            }
            if (mode == 0 && bestClusterTypes.contains(tok.getId())) {
              observedClusterTypes.add(tok.getId());
            }

            // Operate according to the modes
            if (mode == 0) {
              preamble.add(tok);
            } else if (mode == 1) {
              curMiddle.add(tok);
              if (curMiddle.size() == bestClusterTypes.size()) {
                middleGroup.add(curMiddle);
                curMiddle = new ArrayList<Token.AbstractToken>();
              }
            } else {
              if (curMiddle != null) {
                for (Token.AbstractToken t2: curMiddle) {
                  postamble.add(t2);
                }
                curMiddle = null;
              }
              postamble.add(tok);
            }
          }
          preambles.add(preamble);
          middles.addAll(middleGroup);
          postambles.add(postamble);
        }
        //System.err.println("ARRAY-1");
        return new ArrayProphecy(preambles, middles, postambles);
      } else {
        //System.err.println("UNION-2");
        //
        // Well OK, so it's not an ARRAY.  It's none of the above: UNION.
        //
        // We need to partition the chunks somehow, so that later stages can make some kind of progress.
        // In the absence of anything better, we'll group chunks according to the first token in each chunk.
        // If the first token is identical, we'll keep reading until tokens are non-identical.
        // (Note that the paper in bullet #5 says to simply use the first token in each chunk; but this condition 
        // isn't sufficient to guarantee that the algorithm makes progress.)
        //
        // We start by computing how many tokens we need to read before we detect a difference
        int neededTokens = 1;
        boolean foundDifference = false;
        while (! foundDifference) {
          String lastSeenProfile = null;
          for (List<Token.AbstractToken> chunk: chunks) {
            // Build type profile
            StringBuffer curTypeProfile = new StringBuffer();
            for (int i = 0; i < neededTokens; i++) {
              if (i < chunk.size()) {
                curTypeProfile.append(chunk.get(i).getId() + "_");
              } else {
                curTypeProfile.append("<none>" + "_");
              }
            }
            if (lastSeenProfile == null) {
              lastSeenProfile = curTypeProfile.toString();
            } else {
              if (! lastSeenProfile.equals(curTypeProfile.toString())) {
                foundDifference = true;
                break;
              }
            }
          }
          neededTokens++;
        }

        //
        // Now that we have computed 'neededTokens', we can proceed to partition the input chunks using
        // the first 'neededTokens' from each chunk.
        //
        Map<String, List<List<Token.AbstractToken>>> unionMap = new HashMap<String, List<List<Token.AbstractToken>>>();
        for (List<Token.AbstractToken> chunk: chunks) {
          StringBuffer curTypeProfile = new StringBuffer();
          for (int i = 0; i < neededTokens; i++) {
            if (i < chunk.size()) {
              Token.AbstractToken tok = chunk.get(i);
              curTypeProfile.append(tok.getId() + "_");
            } else {
              curTypeProfile.append("<none>" + "_");
            }
          }
          List<List<Token.AbstractToken>> unionChunks = unionMap.get(curTypeProfile.toString());
          if (unionChunks == null) {
            unionChunks = new ArrayList<List<Token.AbstractToken>>();
            unionMap.put(curTypeProfile.toString(), unionChunks);
          }
          unionChunks.add(chunk);
        }

        List<List<List<Token.AbstractToken>>> unionChunklists = new ArrayList<List<List<Token.AbstractToken>>>();
        for (List<List<Token.AbstractToken>> unionChunklist: unionMap.values()) {
          unionChunklists.add(unionChunklist);
        }
        return new UnionProphecy(unionChunklists);
      }
    }
  }

  /**
   */
  private static InferredType discover(List<List<Token.AbstractToken>> chunks) {
    // Remove chunks that are empty.  These should never get passed-in
    for (Iterator<List<Token.AbstractToken>> it = chunks.iterator(); it.hasNext(); ) {
      List<Token.AbstractToken> chunk = it.next();
      if (chunk.size() == 0) {
        it.remove();
      }
    }

    // Error testing
    assert(chunks.size() > 0);

    //
    // Type predictions from the oracle come in one of four flavors: Base, Struct, Array, or Union
    //
    TypeProphecy typePrediction = oracle(chunks);
    if (typePrediction instanceof BaseProphecy) {
      BaseProphecy bp = (BaseProphecy) typePrediction;
      List<String> sampleStrs = new ArrayList<String>();
      for (Iterator<Token.AbstractToken> it = bp.getSamples().iterator(); it.hasNext(); ) {
        Token.AbstractToken tok = it.next();
        sampleStrs.add(tok.getSampleString());
      }
      return new BaseType(bp.getToken(), sampleStrs);

    } else if (typePrediction instanceof StructProphecy) {
      StructProphecy sp = (StructProphecy) typePrediction;
      List<InferredType> structDataTypes = new ArrayList<InferredType>();
      int i = 0;
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

  /////////////////////////////////////////////////////
  // main() tests the TypeInference mechanism
  /////////////////////////////////////////////////////
  public static void main(String argv[]) throws IOException {
    if (argv.length < 1) {
      System.err.println("Usage: TypeInference <datafile> (-verbose)");
      return;
    }
    File f = new File(argv[0]).getCanonicalFile();
    boolean verbose = false;
    for (int i = 1; i < argv.length; i++) {
      if ("-verbose".equals(argv[i])) {
        verbose = true;
      }
    }
    System.err.println("Input file: " + f.getCanonicalPath());

    // Store parse results
    List<List<Token.AbstractToken>> allChunks = new ArrayList<List<Token.AbstractToken>>();

    // Transform the text into a list of "chunks".  
    // A single chunk corresponds to a line of text.  A chunk is a list of Tokens.
    int totalCount = 0;
    int parsedCount = 0;
    long startRead = System.currentTimeMillis();
    BufferedReader in = new BufferedReader(new FileReader(f));
    try {
      String s = in.readLine();
      while (s != null) {
        List<Token.AbstractToken> chunkToks = Tokenizer.tokenize(s);
        if (chunkToks != null) {
          allChunks.add(chunkToks);
          parsedCount++;
        }
        s = in.readLine();
        totalCount++;
      }
    } finally {
      in.close();
    }
    int errorCount = totalCount - parsedCount;

    System.err.println();
    System.err.println("Total lines: " + totalCount);
    System.err.println("Parsed lines: " + parsedCount + " (" + (1.0*parsedCount / totalCount) + ")");
    System.err.println("Error lines: " + errorCount + " (" + (1.0*errorCount / totalCount) + ")");

    //
    // Infer type structure from the tokenized chunks
    //
    long start = System.currentTimeMillis();
    InferredType typeTree = TypeInference.infer(allChunks);
    long end = System.currentTimeMillis();
    double loadTime = (start - startRead) / 1000.0;
    double inferTime = (end - start) / 1000.0;
    double totalTime = (end - startRead) / 1000.0;
    System.err.println("Total elapsed time: " + totalTime);
    System.err.println("Elapsed load time: " + loadTime);
    System.err.println("Elapsed inference time: " + inferTime);
    System.err.println("Ratio load-to-inference: " + (loadTime / inferTime));

    //
    // Dump type structure for debugging
    //
    System.err.println();
    System.err.println("-- Inferred Type Structure -----------------");
    System.err.println(typeTree.toString());


    //
    // What do we want back from the parsed structure?
    // 1) A JSON record for the actual data.
    // 2) Maybe some record of which union branches were parsed?
    //
  }
}
