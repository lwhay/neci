package org.apache.trevni.avro.update;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class BTreeTest {
  public static void main(String[] args) throws IOException{
    DB db = DBMaker.fileDB(new File("/home/ly/mapdb/treetest.db")).make();
    BTreeMap tree = db.treeMap("test").create();
    Schema schema = new Schema.Parser().parse("{\"name\": \"Lineitem\", \"type\": \"record\", \"fields\":[{\"name\": \"l_orderkey\", \"type\": \"long\"}, {\"name\": \"l_linenumber\", \"type\": \"int\"}]}");
    File file = new File(args[0]);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    Record record = new Record(schema);
    String line;
    while((line = reader.readLine()) != null){
      String[] tmp = line.split("|", 5);
      record.put(0, Long.parseLong(tmp[0]));
      record.put(1, Integer.parseInt(tmp[3]));
      tree.put(Long.parseLong(tmp[0]), record);
    }
    reader.close();
    db.commit();//提交持久化
    System.out.println(tree);
    db.close();
  }
}