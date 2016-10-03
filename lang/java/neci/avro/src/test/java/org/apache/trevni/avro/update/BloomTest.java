package org.apache.trevni.avro.update;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.update.BloomFilter.BloomFilterBuilder;

public class BloomTest {
  public static void lkTest(String path, String bloompath, long numElements) throws IOException{
    Schema lkschema = new Schema.Parser().parse("{\"name\": \"Lineitem\", \"type\": \"record\", \"fields\":[{\"name\": \"l_orderkey\", \"type\": \"long\"}, {\"name\": \"l_linenumber\", \"type\": \"int\"}]}");
    int numBucketsPerElement = BloomCount.maxBucketPerElement(numElements);
    BloomFilterModel model = BloomCount.computeBloomModel(numBucketsPerElement, 0.01);
    File bloomfile = new File(bloompath);
    if(!bloomfile.exists()){
      bloomfile.mkdir();
    }
    BloomFilter filter = new BloomFilter(bloomfile, lkschema);
    BloomFilterBuilder builder = filter.creatBuilder(numElements, model.getNumHashes(), model.getNumBucketsPerElement());
    File file = new File(path);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    Record record = new Record(lkschema);
    String line;
    while((line = reader.readLine()) != null){
      String[] tmp = line.split("\\|", 5);
      record.put(0, Long.parseLong(tmp[0]));
      record.put(1, Integer.parseInt(tmp[3]));
      builder.add(record);
    }
    reader.close();
    builder.write();
    filter.activate();
    long t = 0;
    long f = 0;
    BufferedReader checkReader = new BufferedReader(new FileReader(file));
    long[] hashes = new long[2];
    while((line = checkReader.readLine()) != null){
        String[] tmp = line.split("\\|", 5);
        record.put(0, Long.parseLong(tmp[0]));
        record.put(1, Integer.parseInt(tmp[3]));
        boolean x = filter.contains(record, hashes);
        if(x){
          t++;
        }else{
          f++;
        }
      }
    checkReader.close();
    System.out.print("true: "+t+"\tfalse: "+f);
  }

  public static void main(String[] args) throws IOException{
    lkTest(args[0], args[1], Long.parseLong(args[2]));
  }
}
