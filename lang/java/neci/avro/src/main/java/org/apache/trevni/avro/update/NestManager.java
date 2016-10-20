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
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.TrevniRuntimeException;
import org.apache.trevni.avro.update.BloomFilter.BloomFilterBuilder;

public class NestManager {
  private NestSchema[] schemas;
  private Schema nestKeySchema;
  private String resultPath;
  private String tmpPath;
  private int level;
  private int free;
  private int mul;

  private BTreeRecord[] offsetTree;
  private BTreeRecord[] backTree;
  private BTreeRecord forwardTree;

  private BloomFilter[] filter;

  private ColumnReader<Record> reader;

  public NestManager(NestSchema[] schemas, String tmppath, String resultPath, int free, int mul) throws IOException{
    this.schemas = schemas;
    this.tmpPath = tmppath;
    this.resultPath = resultPath;
    level = schemas.length;
    this.free = free;
    this.mul = mul;
    create();
  }

  public void create() throws IOException{
    offsetTree = new BTreeRecord[level];
    backTree = new BTreeRecord[level - 1];
    filter = new BloomFilter[level];
    new File(resultPath).mkdirs();

    Field keyField = null;
    for(int i = level - 1; i > 0; i--){
      Schema s = schemas[i].getNestedSchema();
      filter[i] = new BloomFilter(schemas[i].getBloomFile(), s, schemas[i].getKeyFields());
      List<Field> fs = schemas[i].getSchema().getFields();
      List<Field> keyFields = new ArrayList<Field>();
      List<Field> fieldsCopy = new ArrayList<Field>();
      for(int m : schemas[i].getKeyFields()){
        Field f = fs.get(m);
        keyFields.add(new Schema.Field(f.name(), f.schema(), null, null));
        fieldsCopy.add(new Schema.Field(f.name(), f.schema(), null, null));
      }
      offsetTree[i] = new BTreeRecord(Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, keyFields), new File(resultPath + s.getName() + ".db"), "btree");
      if(keyField != null){
        fieldsCopy.add(keyField);
      }
      keyField = new Schema.Field(s.getName() + "Arr", Schema.createArray(Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, fieldsCopy)), null, null);
      List<Field> fields = new ArrayList<Field>();
      for(Field f : schemas[i - 1].getSchema().getFields()){
        fields.add(new Schema.Field(f.name(), f.schema(), null, null));
      }
      fields.add(new Schema.Field(s.getName() + "Arr", Schema.createArray(s), null, null));
      s = schemas[i-1].getSchema();
      schemas[i-1].setNestedSchema(Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, fields));
      backTree[level - i - 1] = new BTreeRecord(schemas[i].getKeyFields(), schemas[i].getOutKeyFields(), schemas[i].getSchema(), schemas[i].getBTreeFile(),  "btree");
    }
    Schema s = schemas[0].getSchema();
    filter[0] = new BloomFilter(schemas[0].getBloomFile(), s, schemas[0].getKeyFields());
    List<Field> fs = s.getFields();
    List<Field> keyFields = new ArrayList<Field>();
    List<Field> fieldsCopy = new ArrayList<Field>();
    for(int m : schemas[0].getKeyFields()){
      Field f = fs.get(m);
      keyFields.add(new Schema.Field(f.name(), f.schema(), null, null));
      fieldsCopy.add(new Schema.Field(f.name(), f.schema(), null, null));
    }
    offsetTree[0] = new BTreeRecord(Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, keyFields), new File(resultPath + s.getName() + ".db"), "btree");
    if(level > 1){
      fieldsCopy.add(keyField);
      nestKeySchema = Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, fieldsCopy);
      forwardTree = new BTreeRecord(nestKeySchema, new File(resultPath + "keys.db"), "btree");
    }else{
      nestKeySchema = Schema.createRecord(s.getName(), s.getDoc(), s.getNamespace(), false, fieldsCopy);
    }
  }

  public BloomFilterBuilder createBloom(File bloomFile, Schema schema, int[] keyFields, long numElements, int index) throws IOException{
    BloomFilterModel model = BloomCount.computeBloomModel(BloomCount.maxBucketPerElement(numElements),  0.01);
    return filter[index].creatBuilder(numElements, model.getNumHashes(), model.getNumBucketsPerElement());
  }

  public void load() throws IOException{
    assert level > 1;
    if(level == 2){
    //直接以列存方式输出
      dLoad(schemas[1], schemas[0]);
    }else{
      //两个普通文件根据连接关键字排序
      prLoad(schemas[level - 1], schemas[level - 2]);   //读两个普通文件，行存方式输出
      for(int i = level - 2; i > 1; i++){
        //按照连接关键字排序
        orLoad(schemas[i], schemas[i - 1], i);
      }
      laLoad(schemas[1], schemas[0]);
    }
  }

  public boolean search(Record key, Schema valueSchema) throws IOException{
    String name = key.getSchema().getName();
    assert name.equals(valueSchema.getName());
    int le = 0;
    while(le < level){
      if(name.equals(schemas[le].getSchema().getName())){
        break;
      }
      le++;
    }
    if(!filter[le].isActivated()){
      filter[le].activate();
    }
    if(filter[le].contains(key, new long[2])){
      Object v;
      if((v = offsetTree[le].get(key)) != null){
        if(reader == null){
          reader = new ColumnReader<Record>(new File(resultPath + "result.trv"));
        }
        long row = Long.parseLong(v.toString());
        Record res = reader.search(valueSchema, row);
        System.out.println(res);
        return true;
      }
    }
//    System.out.println("The key is not existed:" + key);
    return false;
  }

  public void load(NestSchema schema) throws IOException{
    File file = schema.getPrFile();
    int[] keyFields = schema.getKeyFields();
    Schema s = schema.getSchema();
    long start = System.currentTimeMillis();
    BufferedReader reader = new BufferedReader(new FileReader(file));
    InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(s, resultPath, keyFields, free, mul);
    String line;
    while((line = reader.readLine()) != null){
      String[] tmp = line.split("\\|");
      Record record = arrToRecord(tmp, s);
      writer.append(new ComparableKey(record, keyFields), record);
    }
    reader.close();
    int index = writer.flush();
    File[] files = new File[index];
    for(int i = 0; i < index; i++){
      files[i] = new File(resultPath + "file" + String.valueOf(i)+".trv");
    }
    if(index == 1){
      merge(files);
      new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
      new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
    }else{
      writer.mergeFiles(merge(files), files);
    }

    deleteFile(schema.getPrFile().getAbsolutePath());
    long end = System.currentTimeMillis();
    System.out.println(schema.getSchema().getName() + "\tsort trevni time: " + (end - start) + "ms");
  }

  public void dLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    int[] fields1 = keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields());
    long numElements1 = toSortAvroFile(schema1, fields1);
    long numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());

    BloomFilterBuilder builder1 = createBloom(schema1.getBloomFile(), schema1.getSchema(), schema1.getKeyFields(), numElements1, 1);
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2, 0);
//    backTree[0] = new BTreeRecord(schema1.getKeyFields(), schema1.getOutKeyFields(), schema1.getSchema(), schema1.getBTreeFile(),  "btree");

    //将file1,file2按照file1的外键排序
//    BufferedReader reader1 = new BufferedReader(new FileReader(file1));
//    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeSchema(), fields1);
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(schema2.getNestedSchema(), resultPath,  schema2.getKeyFields(), free, mul);

    Record record1 = reader1.next();
    builder1.add(record1);
    backTree[0].put(record1);
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      builder2.add(record2);
      ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(reader1.hasNext()){
            record1 = reader1.next();
            builder1.add(record1);
            backTree[0].put(record1);
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
    builder1.write();
    builder2.write();
    backTree[0].commit();
    reader1.close();
    reader2.close();
    int index = writer.flush();
    File[] files = new File[index];
    for(int i = 0; i < index; i++){
      files[i] = new File(resultPath + "file" + String.valueOf(i)+".trv");
    }
    if(index == 1){
      merge(files);
      new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
      new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
    }else{
      writer.mergeFiles(merge(files), files);
    }

    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort trevni time: " + (end - start) + "ms");
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

    BloomFilterBuilder builder1 = createBloom(schema1.getBloomFile(), schema1.getSchema(), schema1.getKeyFields(), numElements1, (level - 1));
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2, (level - 2));
//    backTree[0] = new BTreeRecord(schema1.getKeyFields(), schema1.getOutKeyFields(), schema1.getSchema(), schema1.getBTreeFile(),  "btree");
//    backTree[1] = new BTreeRecord(schema2.getKeyFields(), schema2.getOutKeyFields(), schema2.getSchema(), schema2.getBTreeFile(),  "btree");
    //将file1,file2按照file1的外键排序
    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeSchema(), fields1);
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(tmpPath, schema2.getEncodeNestedSchema(), free, mul);
    int[] sortFields = keyJoin(schema2.getOutKeyFields(), schema2.getKeyFields());

    Record record1 = reader1.next();
    builder1.add(record1);
    backTree[0].put(record1);
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      builder2.add(record2);
      backTree[1].put(record2);
      ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
        if(k2.compareTo(k1) == 0){
          arr.add(record1);
          if(reader1.hasNext()){
            record1 = reader1.next();
            builder1.add(record1);
            backTree[0].put(record1);
            continue;
          }else{
            break;
          }
        }else{
          break;
        }
      }
      Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
      writer.append(new ComparableKey(record, sortFields), record);
    }
    builder1.write();
    builder2.write();
    backTree[0].commit();
    backTree[1].commit();
    reader1.close();
    reader2.close();
    writer.flush();
    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
    moveTo(tmpPath, schema2.getPath());
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
  }

  public void orLoad(NestSchema schema1, NestSchema schema2, int index) throws IOException{
    long numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2, (index - 1));
    int bin = level - index - 1;
//    backTree[index] = new BTreeRecord(schema2.getKeyFields(), schema2.getOutKeyFields(), schema2.getSchema(), schema2.getBTreeFile(),  "btree");
    //将file1,file2按照file1的外键排序
    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeNestedSchema(), keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields()));
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(tmpPath, schema2.getEncodeNestedSchema(), free, mul);
    int[] sortFields = keyJoin(schema2.getOutKeyFields(), schema2.getKeyFields());
    //BufferedWriter out = new BufferedWriter(new FileWriter(new File("/home/ly/tmp.avro")));

    Record record1 = reader1.next();
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      builder2.add(record2);
      backTree[bin].put(record2);
      ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
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
      writer.append(new ComparableKey(record, sortFields), record);
    }
    backTree[bin].commit();
    builder2.write();
    reader1.close();
    reader2.close();
    writer.flush();
    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
    moveTo(tmpPath, schema2.getPath());
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
  }

  public void laLoad(NestSchema schema1, NestSchema schema2) throws IOException{
    long numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());
    BloomFilterBuilder builder2 = createBloom(schema2.getBloomFile(), schema2.getSchema(), schema2.getKeyFields(), numElements2, 0);

    long start = System.currentTimeMillis();
    SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeNestedSchema(), keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields()));
    SortedAvroReader reader2 = new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
    InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(schema2.getNestedSchema(), resultPath, schema2.getKeyFields(), free, mul);

    Record record1 = reader1.next();
    while(reader2.hasNext()){
      Record record2 = reader2.next();
      builder2.add(record2);
      ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
      List<Record> arr = new ArrayList<Record>();
      while(true){
        ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
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
    builder2.write();
    reader1.close();
    reader2.close();
    int index = writer.flush();
    File[] files = new File[index];
    for(int i = 0; i < index; i++){
      files[i] = new File(resultPath + "file" + String.valueOf(i)+".trv");
    }
    if(index == 1){
      merge(files);
      new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
      new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
    }else{
      writer.mergeFiles(merge(files), files);
    }

    deleteFile(schema1.getPath());
    deleteFile(schema2.getPath());
    long end = System.currentTimeMillis();
    System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort trevni time: " + (end - start) + "ms");
  }

  public int[] merge(File[] files) throws IOException{
    long t1 = System.currentTimeMillis();
    int level = schemas.length;
    int[] index = new int[schemas.length];
//    Schema s = nestKeySchema;
    int[] la = new int[level - 1];
    for(int i = 0; i < la.length; i++){
      la[i] = schemas[i].getKeyFields().length;
    }
    SortTrevniReader re;
    if(files.length > 1){
      re = new SortTrevniReader(files, nestKeySchema, true);
    }else{
      re = new SortTrevniReader(files, nestKeySchema, false);
    }
    while(re.hasNext()){
      Record record = re.next().getRecord();
      if(level > 1)  forwardTree.put(record, writeKeyRecord(record));
      offsetTree[0].put(record, String.valueOf(index[0]));
      index[0]++;
      List<Record> rs = (List<Record>)record.get(la[0]);
      for(int i = 1; i < level - 1; i++){
        List<Record> tmp = new ArrayList<Record>();
        tmp.addAll(rs);
        rs.clear();
        for(int k = 0; k < tmp.size(); k++){
          Record r = tmp.get(k);
          rs.addAll((List<Record>)r.get(la[i]));
          offsetTree[i].put(r, String.valueOf(index[i]));
          index[i]++;
        }
      }
      for(int k = 0; k < rs.size(); k++){
        offsetTree[level - 1].put(rs.get(k), String.valueOf(index[level - 1]));
        index[level - 1]++;
      }
    }
    if(level > 1)  forwardTree.commit();
    for(int i = 0; i < offsetTree.length; i++){
      offsetTree[i].commit();
    }
    re.close();
    long t2 = System.currentTimeMillis();
    System.out.println("$$$merge read + btree time" + (t2 - t1));
    return re.getGap();
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

  public static String writeKeyRecord(Record record){
    StringBuilder str = new StringBuilder();
    str.append("{");
    Schema s = record.getSchema();
    List<Field> fs = s.getFields();
    int len = fs.size();
    if(len > 0){
      for(int i = 0; i < len; i++){
        if(isSimple(fs.get(i))){
          str.append(record.get(i));
        }else{
          if(fs.get(i).schema().getType() == Type.ARRAY){
            List<Record> rs= (List<Record>)record.get(i);
            int l = rs.size();
            if(l > 0){
              str.append(writeKeyRecord(rs.get(0)));
              for(int j = 1; j < l; j++){
                str.append(",");
                str.append(writeKeyRecord(rs.get(j)));
              }
            }
          }
        }
        if(i < len - 1){
          str.append("|");
        }
      }
    }
    str.append("}");
    return str.toString();
  }

  public static Record readKeyRecord(Schema s, String str){
    char[] ss = str.toCharArray();
    assert(ss[0] == '{' && ss[ss.length - 1] == '}');
    int index = 1;
    Record r = new Record(s);
    int i = 0;
    for(Field f : s.getFields()){
      if(isSimple(f)){
        StringBuilder v = new StringBuilder();
        while(index < (ss.length - 1) && ss[index] != '|'){
          v.append(ss[index]);
          index++;
        }
        r.put(i++, getValue(f, v.toString()));
      }else{
        List<Record> record = new ArrayList<Record>();
        while(index < ss.length - 1){
          assert(ss[index] == '{');
          index++;
          int tt = 1;
          StringBuilder xx = new StringBuilder();
          xx.append('{');
          while(tt > 0){
            if(ss[index] == '{'){
              tt++;
            }
            if(ss[index] == '}'){
              tt--;
            }
            xx.append(ss[index]);
            index++;
          }
          record.add(readKeyRecord(f.schema().getElementType(), xx.toString()));
          if(ss[index] == ','){
            index++;
          }else{
            break;
          }
        }
        r.put(i++, record);
      }
      if(i < s.getFields().size()){
        assert(ss[index] == '|');
        index++;
      }
    }
    assert(index == ss.length - 1);
    return r;
  }

  public static boolean isSimple(Field f){
    switch(f.schema().getType()){
    case INT:  case  LONG:  case STRING:  case BYTES:
      return true;
    case ARRAY:
      return false;
    }
    throw new ClassCastException("cannot support the key type:" + f.schema().getType());
  }

  public static Object getValue(Field f, String s){
    switch(f.schema().getType()){
    case INT:
      return Integer.parseInt(s);
    case LONG:
      return Long.parseLong(s);
    case STRING:
    case BYTES:
      return s;
    }
    throw new ClassCastException("cannot support the key type:" + f.schema().getType());
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
    SortedAvroWriter<ComparableKey, Record> writer = new SortedAvroWriter<ComparableKey, Record>(schema.getPath(), schema.getEncodeSchema(), free, mul);
    String line;
    while((line = reader.readLine()) != null){
      String[] tmp = line.split("\\|");
      numElements++;
      Record record = arrToRecord(tmp, schema.getEncodeSchema());
      writer.append(new ComparableKey(record, keyFields), record);
    }
    reader.close();
    writer.flush();
    deleteFile(schema.getPrFile().getAbsolutePath());
    long end = System.currentTimeMillis();
    System.out.println(schema.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
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
