package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;

public class SortedAvroReader {
  private File[] files;
  private int numFiles;
  private Schema schema;
  private int[] sortKeyFields;
  private AvroReader[] readers;
  private Record[] sortedRecord;
  private int[] noR;
  private int start = 0;

  public SortedAvroReader(String path, Schema schema, int[] keyFields) throws IOException{
    File fpath = new File(path);
    this.files = fpath.listFiles();
    this.numFiles = files.length;
    this.schema = schema;
    this.sortKeyFields = keyFields;
    create();
  }

  public void create() throws IOException{
    readers = new AvroReader[numFiles];
    sortedRecord = new Record[numFiles];
    noR = new int[numFiles];
    for(int i = 0; i < numFiles; i++){
      readers[i] = new AvroReader(schema, files[i]);
      sortedRecord[i] = readers[i].next();
      noR[i] = i;
    }
    Record tmp[] = sortedRecord;
    for(int i = 0; i < numFiles - 1; i++){
      for(int j = i + 1; j < numFiles; j++){
      ComparableKey k1 = new ComparableKey(tmp[i], sortKeyFields);
      ComparableKey k2 = new ComparableKey(tmp[j], sortKeyFields);
      if(k1.compareTo(k2) > 0){
        int tmpNo = noR[i];
        noR[i] = noR[j];
        noR[j] = tmpNo;
        Record tmpR = tmp[i];
        tmp[i] = tmp[j];
        tmp[j] = tmpR;
          }
        }
      }
  }

  public Record next(){
    Record r = sortedRecord[noR[start]];
    if(!readers[noR[start]].hasNext()){
      start++;
    }else{
      sortedRecord[noR[start]] = readers[noR[start]].next();
      int m = start;
      ComparableKey key = new ComparableKey(sortedRecord[noR[start]], sortKeyFields);
      for(int i = start + 1; i < numFiles; i++){
        if(key.compareTo(new ComparableKey(sortedRecord[noR[i]], sortKeyFields)) > 0){
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
    return r;
  }

  public boolean hasNext(){
    if(start < numFiles)    return true;
    else                                return false;
  }

  public static class AvroReader{
    private DataFileReader<Record> fileReader;

    public AvroReader(Schema schema, File file) throws IOException{
      DatumReader<Record>reader = new GenericDatumReader<Record>(schema);
      fileReader = new DataFileReader<Record>(file, reader);
    }

    public boolean hasNext(){
      return fileReader.hasNext();
    }

    public Record next(){
      return fileReader.next();
    }
  }
}
