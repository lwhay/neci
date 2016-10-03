package org.apache.trevni.avro.update;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.update.BTreeRecord.BTreeCreator;
import org.apache.trevni.avro.update.BloomFilter.BloomFilterBuilder;

public class NestedLoad {
  private NestSchema[] schemas;
  private String resultPath;
  private String tmpPath;

  public NestedLoad(NestSchema[] schemas, String tmppath, String resultPath){
    this.schemas = schemas;
    this.tmpPath = tmppath;
    this.resultPath = resultPath;
    create();
  }

  public void create(){
    for(int i = schemas.length - 1; i > 0; i--){
      Schema s = schemas[i].getNestedSchema();
      List<Field> fields = new ArrayList<Field>();
      for(Field f : schemas[i - 1].getSchema().getFields()){
        fields.add(new Schema.Field(f.name(), f.schema(), null, null));
      }
      fields.add(new Schema.Field((s.getName() + "Arr"), Schema.createArray(s), null, null));
      s = schemas[i-1].getSchema();
      schemas[i-1].setNestedSchema(Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, fields));
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
    int[] fields1 = keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields());
    long numElements1 = toSortAvroFile(schema1, fields1);
    long numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());

    BloomFilterBuilder builder1 = createBloom(schema1.getBloomFile(), schema1.getSchema(), schema1.getKeyFields(), numElements1);
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2);

    boolean x = btreeCreate(schema1.getKeyFields(), schema1.getOutKeyFields());
    if(x){
      BTreeRecord btree = new BTreeRecord(schema1.getKeyFields(), schema1.getOutKeyFields(), schema1.getSchema(), schema1.getBTreeFile(),  "btree");
      BTreeCreator creator = btree.createBTree();
    }

    //将file1,file2按照file1的外键排序
//    BufferedReader reader1 = new BufferedReader(new FileReader(file1));
//    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeSchema(), fields1);
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    InsertAvroColumnWriter<CombKey, Record> writer = new InsertAvroColumnWriter<CombKey, Record>(schema2.getNestedSchema(), resultPath, 4,  schema2.getKeyFields());

    Record record1 = reader1.next();
    builder1.add(record1);
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      builder2.add(record2);
      CombKey k2 = new CombKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        CombKey k1 = new CombKey(record1, schema1.getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(reader1.hasNext()){
            record1 = reader1.next();
            builder1.add(record1);
            continue;
          }else{
            break;
          }
        }else{
          break;
        }
      }
      Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
      writer.append(k2, record);
    }
    writer.flush();
    builder1.write();
    builder2.write();
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
  }

  public Record join(Schema schema, Record record, List<Record> arr){
    Record result = new Record(schema);
    List<Field> fs = schema.getFields();
    for(int i = 0; i < fs.size() - 1; i++){
      result.put(i, record.get(i));
//      switch(fs.get(i).schema().getType()){
//      case STRING:  {result.put(i, record.get(i));  break;}
//      case BYTES:  {result.put(i, ByteBuffer.wrap(record.get(i).toString().getBytes()));  break;  }
//      case INT:  {result.put(i, Integer.parseInt(record.get(i).toString()));  break;  }
//      case LONG:  {result.put(i, Long.parseLong(record.get(i).toString()));  break;  }
//      case FLOAT:  {result.put(i, Float.parseFloat(record.get(i).toString()));  break;  }
//      case DOUBLE:  {result.put(i, Double.parseDouble(record.get(i).toString()));  break;  }
//      case BOOLEAN:  {result.put(i, Boolean.getBoolean(record.get(i).toString()));  break;  }
//      default:  {throw new ClassCastException("This type "+fs.get(i).schema().getType()+" is not supported!");  }
//      }
    }
    result.put(fs.size() - 1, arr);
    return result;
  }

  public void prLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    int[] fields1 = keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields());
    long numElements1 = toSortAvroFile(schema1, fields1);
    long numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());

    BloomFilterBuilder builder1 = createBloom(schema1.getBloomFile(), schema1.getSchema(), schema1.getKeyFields(), numElements1);
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2);
    //将file1,file2按照file1的外键排序
    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeSchema(), fields1);
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    SortedAvroWriter writer = new SortedAvroWriter(tmpPath, 4, schema2.getEncodeNestedSchema(), keyJoin(schema2.getOutKeyFields(), schema2.getKeyFields()));

    Record record1 = reader1.next();
    builder1.add(record1);
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      CombKey k2 = new CombKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        CombKey k1 = new CombKey(record1, schema1.getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(reader1.hasNext()){
            record1 = reader1.next();
            builder1.add(record1);
            continue;
          }else{
            break;
          }
        }else{
          break;
        }
      }
      Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
      writer.append(record);
    }
    writer.flush();
    builder1.write();
    builder2.write();
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
    moveTo(tmpPath, schema2.getPath());
  }

  public void orLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    long numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2);
    //将file1,file2按照file1的外键排序
    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeNestedSchema(), keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields()));
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    SortedAvroWriter writer = new SortedAvroWriter(tmpPath, 4, schema2.getEncodeNestedSchema(), keyJoin(schema2.getOutKeyFields(), schema2.getKeyFields()));
    //BufferedWriter out = new BufferedWriter(new FileWriter(new File("/home/ly/tmp.avro")));

    Record record1 = reader1.next();
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      CombKey k2 = new CombKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        CombKey k1 = new CombKey(record1, schema1.getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(reader1.hasNext()){
            record1 = reader1.next();
            continue;
          }else{
            break;
          }
        }else{
          break;
        }
      }
      Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
      writer.append(record);
    }
    writer.flush();
    builder2.write();
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
    moveTo(tmpPath, schema2.getPath());
  }

  public void laLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    long numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2);

    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeNestedSchema(), keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields()));
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    InsertAvroColumnWriter<CombKey, Record> writer = new InsertAvroColumnWriter<CombKey, Record>(schema2.getNestedSchema(), resultPath, 4, schema2.getKeyFields());

    Record record1 = reader1.next();
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      builder2.add(record2);
      CombKey k2 = new CombKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        CombKey k1 = new CombKey(record1, schema1.getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(reader1.hasNext()){
            record1 = reader1.next();
            continue;
          }else{
            break;
          }
        }else{
          break;
        }
      }
      Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
      writer.append(k2, record);
    }
    writer.flush();
    builder2.write();
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
  }

  public boolean btreeCreate(int[] key, int[] value){
    for(int i = 0; i < value.length; i++){
      int j = 0;
      while(j < key.length){
        if(key[j] == value[i]){
          break;
        }else{
          j++;
        }
      }
      if(j == key.length){
        return false;
      }
    }
    return true;
  }

  public void deleteFile(String path){
    File file = new File(path);
    if(file.isDirectory()){
      File[] files = file.listFiles();
      for(int i = 0; i < files.length; i++){
        deleteFile(files[i].getAbsolutePath());
      }
    }else{
      file.delete();
    }
  }

  public void moveTo(String path, String toPath){
    File file = new File(path);
    if(file.isDirectory()){
      File[] files = file.listFiles();
      for(int i = 0; i < files.length; i++){
        files[i].renameTo(new File(toPath + files[i].getName()));
      }
    }
  }

  public long toSortAvroFile(NestSchema schema, int[] keyFields) throws IOException{
    long numElements = 0;
    long start = System.currentTimeMillis();
    File file = schema.getPrFile();
    BufferedReader reader = new BufferedReader(new FileReader(file));
    SortedAvroWriter writer = new SortedAvroWriter(schema.getPath(), 4, schema.getEncodeSchema(), keyFields);
    String line;
    while((line = reader.readLine()) != null){
      String[] tmp = line.split("\\|");
      numElements++;
      writer.append(arrToRecord(tmp, schema.getEncodeSchema()));
    }
    writer.flush();
    reader.close();
    long end = System.currentTimeMillis();
    System.out.println(schema.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
    deleteFile(schema.getPrFile().getAbsolutePath());
    return numElements;
  }

  public int[] keyJoin(int[] key1, int[] key2){
    int len1 = key1.length;
    int len2 = key2.length;
    int[] result = new int[(len1 + len2)];
    for(int i = 0; i < len1; i++){
      result[i] = key1[i];
    }
    for(int i = 0; i < len2; i++){
      result[(i + len1)] = key2[i];
    }
    return result;
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
    reader.close();
    return len;
  }
}
