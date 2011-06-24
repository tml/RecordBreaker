package com.cloudera.recordbreaker.schemadict;

import java.util.Random;

/**
 */
public class TestRecord {
  static Random r = new Random();

  String url;
  int numHits;
  double avgResponseTime;
  public TestRecord() {
    this.url = "http://yahoo.com/" + r.nextInt() + ".html";
    this.numHits = Math.abs(r.nextInt());
    this.avgResponseTime = r.nextDouble();
  }
  public String toString() {
    return url + ", " + numHits + ", " + avgResponseTime;
  }
}
