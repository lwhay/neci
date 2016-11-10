package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

public class SortedAvroWriter<K, V> {
  private String path;
//  private int numFiles;
//  private File[] files;
//  private File tmpFile;
  private Schema schema;
//  private int[] wTimes;
  private SortedArray<K, V> sort = new SortedArray<K, V>();
  private int x = 0;
  private long bytes;

  private int fileIndex = 0;
  private int max;
  private int free;
  private int mul;

  public SortedAvroWriter(String path, Schema schema, int free, int mul){
//    assert numFiles > 1;
    this.path = path;
    this.free = free;
    this.mul = mul;
    bytes = 0;
//    wTimes = new int[numFiles];
    fileDelete(path);
//    createFiles(path, numFiles);
    this.schema = schema;
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

//  public void createFiles(String path, int no){
//    files = new File[no];
//    for(int i = 0; i < no; i++){
//      files[i] = new File(path+"file"+String.valueOf(i)+".avro");
//    }
//    tmpFile = new File(path + "readtmp");
//  }

  public void append(K key, V value) throws IOException{
//    if(!schema.equals(record.getSchema())){
//      throw new IOException("This record does not match the writer schema!!");
//    }
    sort.put(key, value);
    if(x == 0){
      bytes += value.toString().length();
      if(Runtime.getRuntime().freeMemory() <= (free * 1024 * 1024)){
        max = sort.size();
        mul = max / mul;
        System.out.println("####sortarray max####" + mul);
        System.out.println("####max####" + max);
        System.out.println("&&&&bytes&&&&\t" + bytes);
        writeToFile();
      }
    }else{
      if(sort.size() == max){
        writeToFile();
      }
    }
  }

  public void flush() throws IOException{
    if(!sort.isEmpty()){
      writeToFile();
    }
    System.gc();
  }

  public void writeToFile() throws IOException{
    long start = System.currentTimeMillis();
//    if(fileIndex == numFiles){
//      int m = 1;
//      int num = numFiles - 1;
//      while((m + wTimes[num]) >= wTimes[num - 1]){
//        m += wTimes[num];
//        num--;
//        if(num == 0){
//          break;
//        }
//      }
//      fileMerge(num);
//      System.out.println("merge from " + num + " to " + (numFiles - 1));
//      wTimes[num] += m;
//      for(int i = num + 1; i < numFiles; i++){
//        wTimes[i] = 0;
//      }
//    }else{
      DatumWriter<V> writer = new GenericDatumWriter<V>(schema);
      DataFileWriter<V> fileWriter = new DataFileWriter<V>(writer);
      File file = new File(path + "file" + String.valueOf(fileIndex) + ".avro");
      if(!file.getParentFile().exists()){
        file.getParentFile().mkdirs();
      }
      fileWriter.create(schema, file);
      while(sort.size() != 0){
        for(V record : sort.values(mul)){
          fileWriter.append(record);
        }
      }
      fileWriter.close();
//      wTimes[fileIndex]++;
      fileIndex++;
//    }
    long end = System.currentTimeMillis();
    System.out.println("Avro#######" + (++x) + "\ttime: " + (end - start) + "ms");
  }

//  private void fileMerge(int num) throws IOException{
//    fileMerge(num, tmpFile);
//  }
//
//  private void fileMerge(int num, File file) throws IOException{
//    int no = numFiles - num;
//    File[] fs = new File[no];
//    for(int i = 0; i < no; i++){
//      fs[i] = files[i + num];
//    }
//    SortedAvroReader reader = new SortedAvroReader(fs, schema, sortKeyFields);
//
//    DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
//    DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
//    fileWriter.create(schema, file);
//
//    int index = 0;
//    while(reader.hasNext()){
//      Record r1 = reader.next();
//      CombKey key = new CombKey(r1, sortKeyFields);
//      while(index < sort.size()){
//        Record r2 = sort.get(index);
//        if(key.compareTo(new CombKey(r2, sortKeyFields)) < 0){
//          break;
//        }else{
//          fileWriter.append(r2);
//          index++;
//          continue;
//        }
//      }
//      fileWriter.append(r1);
//    }
//    while(index < sort.size()){
//      fileWriter.append(sort.get(index));
//      index++;
//    }
//    fileWriter.close();
//    sort.clear();
//    for(int i = 0; i < no; i++){
//      fs[i].delete();
//    }
//    tmpFile.renameTo(files[num]);
//    fileIndex = num + 1;
//  }
}
