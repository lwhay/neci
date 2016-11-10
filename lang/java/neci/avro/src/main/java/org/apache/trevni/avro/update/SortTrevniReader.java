package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.update.InsertAvroColumnReader.Params;

public class SortTrevniReader{
  private TrevniReader[] readers;
  private Schema schema;
  private int[] keyFields;
  private ComparableKey[] keys;
  private int[] noR;
  private int start;
  private int[] gap;
  private int index;
  private boolean hasGap;

  public SortTrevniReader(File[] files, Schema s, boolean hasGap) throws IOException{
    readers = new TrevniReader[files.length];
    schema = s;
    int len = s.getFields().size();
    keyFields = new int[len - 1];
    for(int i = 0; i < len - 1; i++){
      keyFields[i] = i;
    }
    keys = new ComparableKey[files.length];
    noR = new int[files.length];
    for(int i = 0; i < files.length; i++){
      readers[i] = new TrevniReader(files[i], s);
      keys[i] = readers[i].nextKey();
      noR[i] = i;
    }
    for(int i = 0; i < files.length  - 1; i++){
      for(int j = i + 1; j < files.length; j++){
        if(keys[i].compareTo(keys[j]) > 0){
          int tmpNo = noR[i];
          noR[i] = noR[j];
          noR[j] = tmpNo;
        }
      }
    }
    start = 0;
    this.hasGap = hasGap;
    if(hasGap){
      createGap();
    }
  }

  public int[] getKeyFields(){
    return keyFields;
  }

  public void createGap() throws IOException{
    int row = 0;
    for(int i = 0; i < readers.length; i++){
      row += readers[i].getRowcount();
    }
    gap = new int[row];
    index = 0;
//    int i = 0;
//    while(start < readers.length){
//      re[i] = next();
//      i++;
//    }
//    assert(row == i);
//    close();
//    return re;
  }

  public int[] getGap(){
    if(hasGap){
      if(index >= gap.length){
        return gap;
      }
    }
    return null;
  }

  public ComparableKey next() throws IOException{
    ComparableKey re = keys[noR[start]];
    if(hasGap)
    gap[index] = noR[start];
    index++;
    if(!readers[noR[start]].hasNextKey()){
      start++;
    }else{
      keys[noR[start]] = readers[noR[start]].nextKey();
      int m = start;
      for(int i = start + 1; i < readers.length; i++){
        if(keys[noR[start]].compareTo(keys[noR[i]]) > 0){
          m++;
        }else{
          break;
        }
      }
      if(m > start){
        int tmpNo = noR[start];
        for(int i = start; i < m; i++){
          noR[i] = noR[i + 1];
        }
        noR[m] = tmpNo;
      }
    }
    return re;
  }

  public boolean hasNext(){
    return (start < readers.length);
  }

  public void close() throws IOException{
    for(int i = 0; i < readers.length; i++){
      readers[i].close();
    }
  }

  class TrevniReader{
    private InsertAvroColumnReader<Record> reader;

    public TrevniReader(File file, Schema s) throws IOException{
      Params param = new Params(file);
      param.setSchema(s);
      reader = new InsertAvroColumnReader<Record>(param);
//      values = new ColumnValues[keyFields.length];
//      types = new ValueType[keyFields.length];
//      for(int i = 0; i < keyFields.length; i++){
//        values[i] = reader.getValues(i);
//        types[i] = values[i].getType();
//      }
    }

    public int getRowcount(){
      return reader.getRowCount();
    }

    public ComparableKey nextKey() throws IOException{
      Record record = new Record(schema);
      record = reader.next();
      return new ComparableKey(record, keyFields);
    }

    public boolean hasNextKey(){
      return reader.hasNext();
    }

    public void close() throws IOException{
      reader.close();
    }
  }
}
