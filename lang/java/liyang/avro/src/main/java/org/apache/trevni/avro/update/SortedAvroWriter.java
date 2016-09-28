package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;

public class SortedAvroWriter {
  private String path;
  private int numFiles;
  private File[] files;
  private File tmpFile;
  private Schema schema;
  private int[] sortKeyFields;
  private int[] wTimes;
  private SortedArray<CombKey, Record> sort = new SortedArray<CombKey, Record>();
  private int x = 0;

  private int fileIndex = 0;
  private static final int MAX = 200000;

  public SortedAvroWriter(String path, int numFiles, Schema schema, int[] keyFields){
    assert numFiles > 1;
    this.path = path;
    this.numFiles = numFiles;
    wTimes = new int[numFiles];
    fileDelete(path);
    createFiles(path, numFiles);
    this.schema = schema;
    this.sortKeyFields = keyFields;
  }

  public String getPath(){
    return path;
  }

  public int getFileIndex(){
    return fileIndex;
  }

  public void fileDelete(String path){
    File file = new File(path);
    if(file.exists() & file.isDirectory()){
      File[] files = file.listFiles();
      for(int i = 0; i < files.length; i++){
        files[i].delete();
      }
    }
  }

  public void createFiles(String path, int no){
    files = new File[no];
    for(int i = 0; i < no; i++){
      files[i] = new File(path+"file"+String.valueOf(i)+".avro");
    }
    tmpFile = new File(path + "readtmp");
  }

  public void append(Record record) throws IOException{
    if(!schema.equals(record.getSchema())){
      throw new IOException("This record does not match the writer schema!!");
    }
    sort.put(new CombKey(record, sortKeyFields),  record);
    if(sort.size() == MAX){
      writeToFile();
    }
  }

  public void flush() throws IOException{
    if(!sort.isEmpty()){
      writeToFile();
    }
  }

  public void writeToFile() throws IOException{
    long start = System.currentTimeMillis();
    if(fileIndex == numFiles){
      int m = 1;
      int num = numFiles - 1;
      while((m + wTimes[num]) >= wTimes[num - 1]){
        m += wTimes[num];
        num--;
        if(num == 0){
          break;
        }
      }
      fileMerge(num);
      System.out.println("merge from " + num + " to " + (numFiles - 1));
      wTimes[num] += m;
      for(int i = num + 1; i < numFiles; i++){
        wTimes[i] = 0;
      }
    }else{
      DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
      DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
      if(!files[fileIndex].getParentFile().exists()){
        files[fileIndex].getParentFile().mkdirs();
      }
      if(!files[fileIndex].exists()){
        files[fileIndex].createNewFile();
      }
      fileWriter.create(schema, files[fileIndex]);
      for(Record record : sort.values()){
        fileWriter.append(record);
      }
      fileWriter.close();
      wTimes[fileIndex]++;
      fileIndex++;
    }
    long end = System.currentTimeMillis();
    System.out.println("Avro#######" + (++x) + "\ttime: " + (end - start) + "ms");
  }

  private void fileMerge(int num) throws IOException{
    fileMerge(num, tmpFile);
  }

  private void fileMerge(int num, File file) throws IOException{
    int no = numFiles - num;
    File[] fs = new File[no];
    for(int i = 0; i < no; i++){
      fs[i] = files[i + num];
    }
    SortedAvroReader reader = new SortedAvroReader(fs, schema, sortKeyFields);

    DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
    DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
    fileWriter.create(schema, file);

    int index = 0;
    while(reader.hasNext()){
      Record r1 = reader.next();
      CombKey key = new CombKey(r1, sortKeyFields);
      while(index < sort.size()){
        Record r2 = sort.get(index);
        if(key.compareTo(new CombKey(r2, sortKeyFields)) < 0){
          break;
        }else{
          fileWriter.append(r2);
          index++;
          continue;
        }
      }
      fileWriter.append(r1);
    }
    while(index < sort.size()){
      fileWriter.append(sort.get(index));
      index++;
    }
    fileWriter.close();
    sort.clear();
    for(int i = 0; i < no; i++){
      fs[i].delete();
    }
    tmpFile.renameTo(files[num]);
    fileIndex = num + 1;
  }
}
