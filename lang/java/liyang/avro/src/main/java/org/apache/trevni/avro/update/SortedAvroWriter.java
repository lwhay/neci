package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;

import org.apache.trevni.avro.update.SortedAvroReader.AvroReader;;

public class SortedAvroWriter {
  private String path;
  private int numFiles;
  private File[] files;
  private File tmpFile;
  private Schema schema;
  private int[] sortKeyFields;
  private MyMap<ComparableKey, Record> sort = new MyMap<ComparableKey, Record>();

  private int fileIndex = 0;
  private static final int MAX = 200000;

  public SortedAvroWriter(String path, int numFiles, Schema schema, int[] keyFields){
    assert numFiles > 1;
    this.path = path;
    this.numFiles = numFiles;
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

  public void writeTo(File file) throws IOException{
    if(fileIndex > 1){
      fileMerge((fileIndex - 1), file);
    }else{
      files[0].renameTo(file);
    }
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
    sort.put(new ComparableKey(record, sortKeyFields),  record);
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
    DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
    DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
    if(!files[fileIndex].getParentFile().exists()){
      files[fileIndex].getParentFile().mkdirs();
    }
    if(!files[fileIndex].exists()){
      files[fileIndex].createNewFile();
    }
    fileWriter.create(schema, files[fileIndex]);
    List<Record> values = sort.values();
    sort.clear();
    for(int i = values.size() - 1; i >= 0; i--){
      fileWriter.append(values.get(i));
    }
    values = null;
    fileWriter.close();
    fileIndex++;
    if(fileIndex == numFiles){
      fileMerge();
      tmpFile.renameTo(files[0]);
      fileIndex = 1;
    }
  }

  private void fileMerge() throws IOException{
    fileMerge(numFiles);
  }

  private void fileMerge(int no) throws IOException{
    fileMerge(no, tmpFile);
  }

  private void fileMerge(int no, File file) throws IOException{
    AvroReader[] readers = new AvroReader[no];
    Record[] sortRecord = new Record[no];
    int[] noR = new int[no];
    for(int i = 0; i < no; i++){
      readers[i] = new AvroReader(schema, files[i]);
      sortRecord[i] = readers[i].next();
      noR[i] = i;
    }

    DatumWriter<Record> writer = new GenericDatumWriter<Record>(schema);
    DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
    fileWriter.create(schema, file);
    //初始排序
    Record[] tmp = sortRecord;
    for(int i = 0; i < no - 1; i++){
      for(int j = i + 1; j < no; j++){
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
    tmp = null;

    int start = 0;
    while(true){
      if(start == no){
        break;
      }
      fileWriter.append(sortRecord[noR[start]]);
      if(!readers[noR[start]].hasNext()){
        start++;
        continue;
      }
      sortRecord[noR[start]] = readers[noR[start]].next();
      int m = start;
      ComparableKey key = new ComparableKey(sortRecord[noR[start]], sortKeyFields);
      for(int i = start + 1; i < no; i++){
        if(key.compareTo(new ComparableKey(sortRecord[noR[i]], sortKeyFields)) <= 0){
          break;
        }else{
          m++;
        }
      }
      if(m != start){
        int tmpNo = noR[start];
//        Record tmpR = sortRecord[start];
        for(int i = start; i < m; i++){
          noR[i] = noR[i + 1];
//          sortRecord[i] = sortRecord[i + 1];
        }
        noR[m] = tmpNo;
//        sortRecord[m] = tmpR;
      }
    }
    fileWriter.close();
    for(int i = 0; i < no; i++){
      files[i].delete();
    }
  }
}
