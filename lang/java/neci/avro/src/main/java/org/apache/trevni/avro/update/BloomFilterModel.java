package org.apache.trevni.avro.update;

public class BloomFilterModel {
  private final int numHashes;
  private final int numBucketsPerElement;

  public BloomFilterModel(int numHashes, int numBucketsPerElement){
    this.numHashes = numHashes;
    this.numBucketsPerElement = numBucketsPerElement;
  }

  public int getNumBucketsPerElement(){
    return numBucketsPerElement;
  }

  public int getNumHashes(){
    return numHashes;
  }
}
