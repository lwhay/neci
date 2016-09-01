package org.apache.trevni.avro.update;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
//import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.trevni.avro.update.BloomFilter.BloomFilterBuilder;

public class NestedLoad {
  private NestSchema[] schemas;
  private static final File TMPFILE1 = new File("/home/ly/tmp1.trv");
  private static final File TMPFILE2 = new File("/home/ly/tmp2.trv");
  private static final String TMPPATH = "/home/ly/tmp/";

  public NestedLoad(NestSchema[] schemas){
    this.schemas = schemas;
    create();
  }

  public void create(){
    for(int i = schemas.length - 1; i > 0; i--){
      Schema s = schemas[i].getSchema();
      List<Field> fields = s.getFields();
      fields.add(new Field(s.getName() + "Arr", Schema.createArray(s), null, null));
      s = schemas[i-1].getSchema();
      schemas[i-1].setSchema(Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, fields));
    }
  }

  public BloomFilterBuilder createBloom(File bloomFile, Schema schema, int[] keyFields, long numElements) throws IOException{
    BloomFilterModel model = BloomCount.computeBloomModel(BloomCount.maxBucketPerElement(numElements),  0.01);
    BloomFilter filter = new BloomFilter(bloomFile, schema, keyFields);
    return filter.creatBuilder(numElements, model.getNumHashes(), model.getNumBucketsPerElement());
  }

  public void load() throws IOException{
    int sLen = schemas.length;
    assert sLen > 1;
    if(sLen == 2){
    //直接以列存方式输出
      dLoad(schemas[1], schemas[0]);
    }else{
      //两个普通文件根据连接关键字排序
      prLoad(schemas[sLen - 1], schemas[sLen - 2]);   //读两个普通文件，行存方式输出
      for(int i = sLen - 2; i > 1; i++){
        //按照连接关键字排序
        orLoad(schemas[i], schemas[i - 1]);
      }
      laLoad(schemas[1], schemas[0]);
    }
  }

  public void dLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    File file1 = schema1.getFile();
    File file2 = schema2.getFile();
    Schema s1 = schema1.getSchema();
    Schema s2 = schema2.getSchema();

    long numElements1 = getNumLines(file1);
    long numElements2 = getNumLines(file2);

    BloomFilterBuilder builder1 = createBloom(schema1.getBloomFile(), schema1.getSchema(), schema1.getKeyFields(), numElements1);
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2);

    //将file1,file2按照file1的外键排序
    BufferedReader reader1 = new BufferedReader(new FileReader(file1));
    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
    InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(s2, TMPFILE1.getAbsolutePath(), TMPFILE2.getAbsolutePath());
    String tmp1 = "";
    String tmp2 = "";

    Record record1 = new Record(s1);
    builder1.add(record1);
    while((tmp2 = reader2.readLine()) != null){
      String[] str2 = tmp2.split("\\|");
      Record record2 = arrToRecord(str2, s2);
      builder2.add(record2);
      ComparableKey k2 = new ComparableKey(record2, schemas[schemas.length - 2].getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        if(tmp1 == ""){
          tmp1 = reader1.readLine();
          String[] str1 = tmp1.split("\\|");
          record1 = arrToRecord(str1, s1);
          builder1.add(record1);
        }

        ComparableKey k1 = new ComparableKey(record1, schemas[schemas.length - 1].getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if((tmp1 = reader1.readLine()) != null){
            continue;
          }
        }
        break;
      }
      record2.put(s2.getFixedSize() - 1, arr);
      writer.append(k2, record2);
    }
    writer.flush();
    builder1.write();
    builder2.write();
    reader1.close();
    reader2.close();
  }

  public void prLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    File file1 = schema1.getFile();
    File file2 = schema2.getFile();
    Schema s1 = schema1.getSchema();
    Schema s2 = schema2.getSchema();
    //将file1,file2按照file1的外键排序
    BufferedReader reader1 = new BufferedReader(new FileReader(file1));
    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
    SortedAvroWriter writer = new SortedAvroWriter(TMPPATH, 4, s2, schema2.getOutKeyFields());
    String tmp1 = "";
    String tmp2 = "";

    Record record1 = new Record(s1);
    while((tmp2 = reader2.readLine()) != null){
      String[] str2 = tmp2.split("\\|");
      Record record2 = arrToRecord(str2, s2);
      ComparableKey k2 = new ComparableKey(record2, schemas[schemas.length - 2].getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        if(tmp1 == ""){
          tmp1 = reader1.readLine();
          String[] str1 = tmp1.split("\\|");
          record1 = arrToRecord(str1, s1);
        }

        ComparableKey k1 = new ComparableKey(record1, schemas[schemas.length - 1].getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if((tmp1 = reader1.readLine()) != null){
            continue;
          }
        }
        break;
      }
      record2.put(s2.getFixedSize() - 1, arr);
      writer.append(record2);
    }
    writer.flush();
    reader1.close();
    reader2.close();
    file2.delete();
    if(!file2.exists())  file2.mkdir();
    writer.writeTo(file2);
  }

  public void orLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    File file1 = schema1.getFile();
    File file2 = schema2.getFile();
    Schema s1 = schema1.getSchema();
    Schema s2 = schema2.getSchema();
    //将file1,file2按照file1的外键排序
    DatumReader<Record> reader1 = new GenericDatumReader<Record>(s1);
    DataFileReader<Record> fileReader = new DataFileReader<Record>(file1, reader1);
    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
    SortedAvroWriter writer = new SortedAvroWriter(TMPPATH, 4, s2, schema2.getOutKeyFields());
    //BufferedWriter out = new BufferedWriter(new FileWriter(new File("/home/ly/tmp.avro")));
    String tmp2 = "";

    Record record1 = fileReader.next();
    while((tmp2 = reader2.readLine()) != null){
      String[] str2 = tmp2.split("\\|");
      Record record2 = arrToRecord(str2, s2);
      ComparableKey k2 = new ComparableKey(record2, schemas[schemas.length - 2].getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        ComparableKey k1 = new ComparableKey(record1, schemas[schemas.length - 1].getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(fileReader.hasNext()){
            record1 = fileReader.next();
            continue;
          }
        }
        break;
      }
      record2.put(s2.getFixedSize() - 1, arr);
      writer.append(record2);
    }
    writer.flush();
    fileReader.close();
    reader2.close();
    file2.delete();
    if(!file2.exists())  file2.mkdir();
    writer.writeTo(file2);
  }

  public void laLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    File file1 = schema1.getFile();
    File file2 = schema2.getFile();
    Schema s1 = schema1.getSchema();
    Schema s2 = schema2.getSchema();
    //将file1,file2按照file1的外键排序
    DatumReader<Record> reader1 = new GenericDatumReader<Record>(s1);
    DataFileReader<Record> fileReader = new DataFileReader<Record>(file1, reader1);
    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
    InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(s2, TMPFILE1.getAbsolutePath(), TMPFILE2.getAbsolutePath());
    String tmp2 = "";

    Record record1 = fileReader.next();
    while((tmp2 = reader2.readLine()) != null){
      String[] str2 = tmp2.split("\\|");
      Record record2 = arrToRecord(str2, s2);
      ComparableKey k2 = new ComparableKey(record2, schemas[schemas.length - 2].getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        ComparableKey k1 = new ComparableKey(record1, schemas[schemas.length - 1].getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(fileReader.hasNext()){
            record1 = fileReader.next();
            continue;
          }
        }
        break;
      }
      record2.put(s2.getFixedSize() - 1, arr);
      writer.append(k2, record2);
    }
    writer.flush();
    fileReader.close();
    reader2.close();
  }

  public Record arrToRecord(String[] arr, Schema s){
    Record record = new Record(s);
    List<Field> fs = s.getFields();
    for(int i = 0; i < arr.length; i++){
      switch(fs.get(i).schema().getType()){
      case STRING:  {record.put(i, arr[i]);  break;  }
      case BYTES:  {record.put(i, ByteBuffer.wrap(arr[i].getBytes()));  break;  }
      case INT:  {record.put(i, Integer.parseInt(arr[i]));  break;  }
      case LONG:  {record.put(i, Long.parseLong(arr[i]));  break;  }
      case FLOAT:  {record.put(i, Float.parseFloat(arr[i]));  break;  }
      case DOUBLE:  {record.put(i, Double.parseDouble(arr[i]));  break;  }
      case BOOLEAN:  {record.put(i, Boolean.getBoolean(arr[i]));  break;  }
      default:  {throw new ClassCastException("This type "+fs.get(i).schema().getType()+" is not supported!");  }
      }
    }
    return record;
  }

  public long getNumLines(File file) throws IOException{
    long len = 0;
    BufferedReader reader = new BufferedReader(new FileReader(file));
    while(reader.readLine() != null){
      len++;
    }
    return len;
  }
}
