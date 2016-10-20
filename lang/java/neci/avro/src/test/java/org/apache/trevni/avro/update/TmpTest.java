package org.apache.trevni.avro.update;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

public class TmpTest {
  public static void main(String[] args){
    Schema s = new Schema.Parser().parse("{\"name\":\"a\", \"type\":\"record\", \"fields\":[{\"name\":\"ak\", \"type\":\"int\"}, {\"name\": \"bA\", \"type\":{\"type\":\"array\", \"items\":{\"name\":\"b\", \"type\":\"record\", \"fields\":[{\"name\":\"bk\", \"type\": \"int\"}, {\"name\":\"cA\", \"type\":{\"type\":\"array\", \"items\":{\"name\":\"c\", \"type\":\"record\", \"fields\":[{\"name\":\"ck\", \"type\":\"int\"}]}}}]}}}]}");
    Schema s1 = s.getFields().get(1).schema().getElementType();
    Schema s2 = s1.getFields().get(1).schema().getElementType();
    Record r = new Record(s);
    List<Record> l1 = new ArrayList<Record>();

    r.put(0, 1);
    for(int i = 0; i < 2; i++){
      Record r1 = new Record(s1);
      r1.put(0, i);
      List<Record> l2 = new ArrayList<Record>();
      for(int j = 0; j < 2; j++){
        Record r2 = new Record(s2);
        r2.put(0, j);
        l2.add(r2);
      }
      r1.put(1, l2);
      l1.add(r1);
    }

    r.put(1, l1);
    String str = NestManager.writeKeyRecord(r);
    Record re = NestManager.readKeyRecord(s, str);
    System.out.println("write:" + str);
    System.out.println("read:" + re);
  }
}
