package org.apache.trevni.avro.update;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

public class SortedAvroTest {
  public static void main(String[] args) throws IOException{
    File file = new File("/home/ly/dbgen/dbgen/lineitem.tbl");
    Schema s = new Schema.Parser().parse(new File("/home/ly/schemas/o_l.avsc"));
//    EncodeSchema e = new EncodeSchema(s);
//    System.out.println(e.getEncode().toString());
    SortedAvroWriter writer = new SortedAvroWriter("/home/ly/avrotest/", 4, s, new int[]{0, 3});
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line = "";
    while((line = reader.readLine()) != null){
      String[] l = line.split("\\|");
      Record lineitem = new Record(s);
      lineitem.put(0, Long.parseLong(l[0]));
      lineitem.put(1, Long.parseLong(l[1]));
      lineitem.put(2, Long.parseLong(l[2]));
      lineitem.put(3, Integer.parseInt(l[3]));
      lineitem.put(4, Float.parseFloat(l[4]));
      lineitem.put(5, Float.parseFloat(l[5]));
      lineitem.put(6, Float.parseFloat(l[6]));
      lineitem.put(7, Float.parseFloat(l[7]));
      lineitem.put(8, ByteBuffer.wrap(l[8].getBytes()));
      lineitem.put(9, ByteBuffer.wrap(l[9].getBytes()));
      lineitem.put(10, l[10]);
      lineitem.put(11, l[11]);
      lineitem.put(12, l[12]);
      lineitem.put(13, l[13]);
      lineitem.put(14, l[14]);
      lineitem.put(15, l[15]);
      writer.append(lineitem);
    }
    reader.close();
    writer.flush();
  }
}
