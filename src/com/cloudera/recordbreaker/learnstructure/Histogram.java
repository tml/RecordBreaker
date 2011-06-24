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

import java.util.*;

/*********************************************************
 * Histogram statistically summarizes the contents of a set of chunks.
 * It's used to find similar, but not identical, chunks in the input data.
 *********************************************************/
public class Histogram {
  ///////////////////////////////////////////////////
  // static members
  ///////////////////////////////////////////////////
  static double CLUSTER_TOLERANCE = 0.01;
  static double MAX_RESIDUAL_MASS = 0.1;
  static double MIN_COVERAGE_FACTOR = 0.2;

  ///////////////////////////////////////////////////
  // static classes
  ///////////////////////////////////////////////////
  static class FrequencyPair implements Comparable {
    int perChunkFrequency;
    int overallChunkCount;
    public FrequencyPair(int perChunkFrequency, int overallChunkCount) {
      this.perChunkFrequency = perChunkFrequency;
      this.overallChunkCount = overallChunkCount;
    }
    public int compareTo(Object o) {
      FrequencyPair other = (FrequencyPair) o;
      int cmp = overallChunkCount - other.overallChunkCount;
      if (cmp != 0) {
        return -1 * cmp;
      } else {
        cmp = perChunkFrequency - other.perChunkFrequency;
        return cmp;
      }
    }
    public int getPerChunkFrequency() {
      return perChunkFrequency;
    }
    public int getCount() {
      return overallChunkCount;
    }
  }

  //////////////////////////////////////////////////////////////
  // Static methods: for computing histograms from data,
  // and for clustering the resulting histograms.
  //////////////////////////////////////////////////////////////
  /**
   * Compute a statistical summary of the data.  This will return a histogram
   * for each token type, indicating the relative proportion and distribution of
   * the token type in the chunkset.  
   *
   * Thus, the size of the output of this function is relatively small: no larger 
   * than the number of potential token types.  However, it can take awhile to compute
   * if the input data size is huge.  
   *
   * REMIND mjc - This fn may be a good candidate for runtime-perf optimization
   */
  public static List<Histogram> computeNormalizedHistograms(List<List<Token.AbstractToken>> chunks) {
    Map<String, Map<Integer, Integer>> allHistograms = new TreeMap<String, Map<Integer, Integer>>();
    List<Histogram> normalizedHistograms = new ArrayList<Histogram>();

    //
    // 1.  Compute some per-chunk statistics
    //
    for (List<Token.AbstractToken> chunk: chunks) {
      // Compute frequencies of token-type within this chunk
      HashMap<String, Integer> localFreq = new HashMap<String, Integer>();
      for (Token.AbstractToken tok: chunk) {
        Integer count = (Integer) localFreq.get(tok.getId());
        if (count == null) {
          localFreq.put(tok.getId(), 1);
        } else {
          localFreq.put(tok.getId(), count.intValue() + 1);
        }
      }

      // Now adjust the "histogram of frequencies" associated with each token type
      for (String tokenId: localFreq.keySet()) {
        Map<Integer, Integer> perTokenTypeHistogram = allHistograms.get(tokenId);
        if (perTokenTypeHistogram == null) {
          perTokenTypeHistogram = new HashMap<Integer, Integer>();
          allHistograms.put(tokenId, perTokenTypeHistogram);
        }
        Integer currentTokenTypeCount = localFreq.get(tokenId);
        Integer countSoFar = perTokenTypeHistogram.get(currentTokenTypeCount);
        if (countSoFar == null) {
          perTokenTypeHistogram.put(currentTokenTypeCount, 1);
        } else {
          perTokenTypeHistogram.put(currentTokenTypeCount, countSoFar.intValue() + 1);
        }
      }
    }

    //
    // 2.  Now for each per-token-type histogram, compute how many times the
    //     token was observed in *no chunk at all*.
    //
    for (String tokenId: allHistograms.keySet()) {
      Map<Integer, Integer> perTokenTypeHistogram = allHistograms.get(tokenId);
      int numberOfChunksForObservedTokenTypeCount = 0;
      for (Integer currentTokenTypeCount: perTokenTypeHistogram.keySet()) {
        numberOfChunksForObservedTokenTypeCount += perTokenTypeHistogram.get(currentTokenTypeCount);
      }
      perTokenTypeHistogram.put(0, chunks.size() - numberOfChunksForObservedTokenTypeCount);
    }

    //
    // 3.  Normalize the per-token-type histograms
    //
    for (Map.Entry<String, Map<Integer, Integer>> e1: allHistograms.entrySet()) {
      String tokenId = e1.getKey();
      Map<Integer, Integer> perTokenTypeHistogram = e1.getValue();
      double coverage = 0;
      double totalMass = 0;

      // 3.1.  Compute the histogram's normal form: all the counts in descending order of prevalence in the chunk set.
      //       Also, compute some metainfo stats along the way
      SortedSet<FrequencyPair> sorter = new TreeSet<FrequencyPair>();
      for (Map.Entry<Integer, Integer> e2: perTokenTypeHistogram.entrySet()) {
        Integer perChunkFrequency = e2.getKey();
        Integer overallChunkCount = e2.getValue();
        if (perChunkFrequency.intValue() != 0) {
          coverage += overallChunkCount.intValue();
          sorter.add(new FrequencyPair(perChunkFrequency, overallChunkCount));
        }
        totalMass += overallChunkCount.intValue();
      }
      List<FrequencyPair> normalForm = new ArrayList<FrequencyPair>();
      for (FrequencyPair p: sorter) {
        normalForm.add(p);
      }
      normalForm.add(0, new FrequencyPair(0, perTokenTypeHistogram.get(0)));

      // 3.2.  Compute metainfo
      double width = perTokenTypeHistogram.size()-1;
      double residualMass = (totalMass - normalForm.get(1).getCount()) / totalMass;

      // 3.3   Done with the histogram!
      normalizedHistograms.add(new Histogram(tokenId, normalForm, width, residualMass, coverage));
    }
    return normalizedHistograms;
  }

  /**
   * Cluster together histograms that appear to be related.
   *
   * We currently employ agglomerative single-link clustering.  That means:
   * a) We can imagine that each data elt starts as its own cluster
   * b) We merge clusters whenever the distance between clusters is less than CLUSTER_TOLERANCE
   * c) The distance between two clusters is determined by the *minimum distance between any two members of the cluster*.
   *    This is sometimes called "single link" clustering.  The resulting cluster quality is not as good as computing 
   *    distance based on the average of the members of a cluster, but it is more efficient.
   */
  public static List<List<Histogram>> clusterHistograms(List<Histogram> inputHistograms) {
    // 1.  Handle degenerate case of size(input) == 1
    if (inputHistograms.size() == 1) {
      List<List<Histogram>> clusters = new ArrayList<List<Histogram>>();
      clusters.add(inputHistograms);
      return clusters;
    }

    // 2.  Otherwise, compute pairwise symmetric relative entropy among histograms
    class Score implements Comparable {
      double s;
      int i;
      int j;
      public Score(double s, int i, int j) {
        this.s = s;
        this.i = i;
        this.j = j;
      }
      public int getIndex1() {
        return i;
      }
      public int getIndex2() {
        return j;
      }
      public int compareTo(Object o) {
        Score other = (Score) o;
        if (this.s < other.s) {
          return -1;
        } else if (this.s > other.s) {
          return 1;
        } else {
          int cmp = this.i - other.i;
          if (cmp == 0) {
            cmp = this.j - other.j;
          }
          return cmp;
        }
      }
    }
    SortedSet<Score> scores = new TreeSet<Score>();
    for (int i = 0; i < inputHistograms.size(); i++) {
      for (int j = i+1; j < inputHistograms.size(); j++) {
        Histogram h1 = inputHistograms.get(i);
        Histogram h2 = inputHistograms.get(j);
        double sre = h1.computeSymmetricRelativeEntropy(h2);
        if (sre < CLUSTER_TOLERANCE) {
          scores.add(new Score(sre, i, j));
        }
      }
    }

    // Initialize clusters
    Map<Integer, Integer> histogramToCluster = new TreeMap<Integer, Integer>();
    Map<Integer, Set<Integer>> clusterToHistograms = new TreeMap<Integer, Set<Integer>>();
    for (int i = 0; i < inputHistograms.size(); i++) {
      histogramToCluster.put(i, i);
      Set<Integer> containedHistograms = new HashSet<Integer>();
      containedHistograms.add(i);
      clusterToHistograms.put(i, containedHistograms);
    } 
    // Start merging clusters
    for (Score s: scores) {
      int idx1 = s.getIndex1();
      int idx2 = s.getIndex2();
      int cluster1 = histogramToCluster.get(idx1);
      int cluster2 = histogramToCluster.get(idx2);

      if (cluster1 == cluster2) {
        continue;
      }
      for (Integer histogramId: clusterToHistograms.get(cluster2)) {
        histogramToCluster.put(histogramId, cluster1);
      }
      clusterToHistograms.get(cluster1).addAll(clusterToHistograms.get(cluster2));
      clusterToHistograms.remove(cluster2);
    }

    // Build the clustered histogram list.
    List<List<Histogram>> clusters = new ArrayList<List<Histogram>>();
    for (Map.Entry<Integer, Set<Integer>> entry: clusterToHistograms.entrySet()) {
      Integer clusterId = entry.getKey();
      Set<Integer> histograms = entry.getValue();

      List<Histogram> curCluster = new ArrayList<Histogram>();
      for (Integer histogramIndex: histograms) {
        curCluster.add(inputHistograms.get(histogramIndex));
      }
      clusters.add(curCluster);
    }
    return clusters;
  }

  //////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////
  String histogramType;
  List<FrequencyPair> normalForm;
  double width;
  double residualMass;
  double coverage;

  //////////////////////////////////////////////////////////////
  // Methods
  //////////////////////////////////////////////////////////////
  public Histogram(String histogramType, List<FrequencyPair> normalForm, double width, double residualMass, double coverage) {
    this.histogramType = histogramType;
    this.normalForm = normalForm;
    this.width = width;
    this.residualMass = residualMass;
    this.coverage = coverage;
  }

  public boolean passStructStatisticalTest(int numChunks) {
    return residualMass < MAX_RESIDUAL_MASS && coverage > MIN_COVERAGE_FACTOR * numChunks;
  }
  public boolean passArrayStatisticalTest(int numChunks) {
    return width > 3 && coverage > MIN_COVERAGE_FACTOR * numChunks;
  }
  public String getHistogramType() {
    return histogramType;
  }
  public double getWidth() {
    return width;
  }
  public double getResidualMass() {
    return residualMass;
  }
  public double getCoverage() {
    return coverage;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("Histogram:  type=" + histogramType + ", width=" + width + ", residualMass=" + residualMass + ", coverage=" + coverage + ", normalForm=[");
    for (FrequencyPair fp: normalForm) {
      buf.append("(" + fp.getPerChunkFrequency() + ", " + fp.getCount() + ") ");
    }
    buf.append("]");
    return buf.toString();
  }

  /**
   * The relative entropy score is used for clustering.  However, we can't compute
   * it directly, as histograms do not always contain the same components.  Instead,
   * we preprocess the data with computeSymmetricRelativeEntropy(), then pass the resulting
   * averaged values into this pretty generic method.
   */
  double computeRelativeEntropy(List<Double> avgCounts) {
    double total = 0;
    for (int i = 1; i < normalForm.size(); i++) {
      double selfOverallChunkCount = normalForm.get(i).getCount();
      double otherOverallChunkCount = avgCounts.get(i);
      total += selfOverallChunkCount * Math.log(selfOverallChunkCount / otherOverallChunkCount);
    }
    return total;
  }

  /**
   * The point of this method is to preprocess the data from two input Histograms,
   * getting it ready for the relative entropy computation.  Without this preprocessing,
   * the rel-entropy computation would be sensitive to varying numbers of components in each histogram.
   */
  double computeSymmetricRelativeEntropy(Histogram other) {
    List<Double> avgCounts = new ArrayList<Double>();
    for (int i = 0; i < Math.max(normalForm.size(), other.normalForm.size()); i++) {
      if ((i < normalForm.size()) && (i < other.normalForm.size())) {
        avgCounts.add((normalForm.get(i).getCount() + other.normalForm.get(i).getCount()) / 2.0);
      } else if (i < normalForm.size()) {
        avgCounts.add(normalForm.get(i).getCount() * 0.5);
      } else {
        avgCounts.add(other.normalForm.get(i).getCount() * 0.5);
      }
    }
    return 0.5 * this.computeRelativeEntropy(avgCounts) + 0.5 * other.computeRelativeEntropy(avgCounts);
  }
}
