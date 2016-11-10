package org.apache.trevni.avro.update;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeKeySerializer.BasicKeySerializer;
import org.mapdb.BTreeKeySerializer.Tuple2KeySerializer;
import org.mapdb.BTreeKeySerializer.Tuple3KeySerializer;
import org.mapdb.BTreeKeySerializer.Tuple4KeySerializer;
import org.mapdb.BTreeKeySerializer.Tuple5KeySerializer;
import org.mapdb.BTreeKeySerializer.Tuple6KeySerializer;

public class BTreeRecord {
  private int[] keyFields;
  private int[] valueFields;
  private Schema schema;
  private Schema valueSchema;
  private File dbFile;
  private String name;
  private int index;
  private static final int MAX = 20000;
  private DB db;
  private BTreeMap mTree;
  private boolean isBtree;

  public BTreeRecord(Schema s, File file, String name){
    this(compFields(s), s, file, name);
  }

  private static int[] compFields(Schema s){
    List<Field> fs = s.getFields();
    int i = 0;
    for(Field f : fs){
      if(NestManager.isSimple(f)){
        i++;
      }else{
        break;
      }
    }
    int[] keyFields = new int[i];
    for(int k = 0; k < i; k++){
      keyFields[k] = k;
    }
    return keyFields;
  }

  public BTreeRecord(int[] keyFields, Schema schema, File file, String name){
    this(keyFields, null, schema, file, name);
  }

  public BTreeRecord(int[] keyFields, int[] valueFields, Schema schema, File file, String name){
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    this.schema = schema;
    if(valueFields != null){
      List<Field> fields = new ArrayList<Field>();
      List<Field> fs = schema.getFields();
      for(int i = 0; i < valueFields.length; i++){
        Field f = fs.get(valueFields[i]);
        Type type = f.schema().getType();
        if(type.compareTo(Type.ARRAY) == 0 | type.compareTo(Type.ENUM) == 0 | type.compareTo(Type.FIXED) == 0 | type.compareTo(Type.MAP) == 0 | type.compareTo(Type.NULL) == 0){
          throw new ClassCastException("This type is not supported: " + type);
        }
        fields.add(new Schema.Field(f.name(), f.schema(), null, null));
      }
      valueSchema = Schema.createRecord(schema.getName(), null, null, false, fields);
    }
    isBtree = isBtree(keyFields, valueFields);
    if(isBtree){
      assert(file != null);
      assert(name != null);
      this.dbFile = file;
      this.name = name;
      create();
    }
    index = 0;
  }

  public boolean isBtree(int[] key, int[] value){
    if(value == null){
      return true;
    }
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
        return true;
      }
    }
    return false;
  }

  public void create(){
    if(dbFile.exists()){
      db = DBMaker.newFileDB(dbFile).make();
      mTree = db.getTreeMap(name);
    }else{
      db = DBMaker.newFileDB(dbFile).make();
      mTree = db.createTreeMap(name).keySerializer(transSer(schema, keyFields)).valueSerializer(Serializer.STRING).make();
    }
  }

  public void put(Record record, boolean isKey){
    put(record, NestManager.writeKeyRecord(transValue(record)), isKey);
  }

  public void put(Record record, String value, boolean isKey){
    if(isBtree){
      if(isKey)
        mTree.put(transKey(record), value);
      else
        mTree.put(transKey(record, keyFields), value);
      index++;
      if(index == MAX){
        commit();
      }
    }
  }

  public void remove(Record record, boolean isKey){
    if(isBtree){
      Object key;
      if(isKey)
        key = transKey(record);
      else
        key = transKey(record, keyFields);
      mTree.remove(key);
    }
  }

  public Object get(Record record, boolean isKey){
    if(isBtree){
      Object key;
      if(record.getSchema().getName().equals(schema.getName()) && !isKey){
        key = transKey(record, keyFields);
      }else{
        key = transKey(record);
      }
      return mTree.get(key);
    }else{
      return transValue(record);
    }
  }

  public Record getRecord(Record record, boolean isKey){
    Object v = get(record, isKey);
    if(v != null){
      if(isBtree)
        return NestManager.readKeyRecord(valueSchema, v.toString());
      else
        return (Record)v;
    }else{
      return null;
    }
  }

  public static BTreeKeySerializer transSer(Schema s, int[] fields){
    Serializer[] sers = new Serializer[fields.length];
    List<Field> fs = s.getFields();
    for(int i = 0; i < sers.length; i++){
      Type type = fs.get(fields[i]).schema().getType();
      switch(type){
      case BOOLEAN:
        sers[i] = Serializer.BOOLEAN;
        break;
      case INT:
        sers[i] = Serializer.INTEGER;
        break;
      case LONG:
        sers[i] = Serializer.LONG;
        break;
      case STRING:
        sers[i] = Serializer.STRING;
        break;
      case BYTES:
        sers[i] = Serializer.BYTE_ARRAY;
        break;
      default:
        throw new ClassCastException("The key field type is not supported:"+type);
      }
    }
    switch(fields.length){
    case 1:
      return new BasicKeySerializer(sers[0]);
    case 2:
      return new Tuple2KeySerializer(null, sers[0], sers[1]);
    case 3:
      return new Tuple3KeySerializer(null, null, sers[0], sers[1], sers[2]);
    case 4:
      return new Tuple4KeySerializer(null, null, null, sers[0], sers[1], sers[2], sers[3]);
    case 5:
      return new Tuple5KeySerializer(null, null, null, null, sers[0], sers[1], sers[2], sers[3], sers[4]);
    case 6:
      return new Tuple6KeySerializer(null, null, null, null, null, sers[0], sers[1], sers[2], sers[3], sers[4], sers[5]);
    default:
      throw new ClassCastException("The key field number is not supported:"+fields.length);
    }
  }

  public Object transKey(Record record){
    Object[] keys = new Object[keyFields.length];
    List<Field> fs = record.getSchema().getFields();
    assert (fs.size() >= keys.length);
    for(int i = 0; i < keyFields.length; i++){
      Type type = fs.get(i).schema().getType();
      switch(type){
      case BOOLEAN:
        keys[i] = Boolean.parseBoolean(record.get(i).toString());
        break;
      case INT:
        keys[i] = Integer.parseInt(record.get(i).toString());
        break;
      case LONG:
        keys[i] = Long.parseLong(record.get(i).toString());
        break;
      case STRING:
        keys[i] = record.get(i).toString();
        break;
      case BYTES:
        keys[i] = record.get(i).toString().getBytes();
        break;
      default:
        throw new ClassCastException("The key field type is not supported:"+type);
      }
    }
    switch(keyFields.length){
    case 1:
      return keys[0];
    case 2:
      return Fun.t2(keys[0], keys[1]);
    case 3:
      return Fun.t3(keys[0], keys[1], keys[2]);
    case 4:
      return Fun.t4(keys[0], keys[1], keys[2], keys[3]);
    case 5:
      return Fun.t5(keys[0], keys[1], keys[2], keys[3], keys[4]);
    case 6:
      return Fun.t6(keys[0], keys[1], keys[2], keys[3], keys[4], keys[5]);
    default:
      throw new ClassCastException("The key field number is not supported:"+keyFields.length);
    }
  }

  public Object transKey(Record record, int[] fields){
    assert (keyFields.length == fields.length);
    Object[] keys = new Object[fields.length];
    List<Field> fs = record.getSchema().getFields();
    for(int i = 0; i < fields.length; i++){
      Type type = fs.get(fields[i]).schema().getType();
      switch(type){
      case BOOLEAN:
        keys[i] = Boolean.parseBoolean(record.get(fields[i]).toString());
        break;
      case INT:
        keys[i] = Integer.parseInt(record.get(fields[i]).toString());
        break;
      case LONG:
        keys[i] = Long.parseLong(record.get(fields[i]).toString());
        break;
      case STRING:
        keys[i] = record.get(fields[i]).toString();
        break;
      case BYTES:
        keys[i] = record.get(fields[i]).toString().getBytes();
        break;
      default:
        throw new ClassCastException("The key field type is not supported:"+type);
      }
    }
    switch(fields.length){
    case 1:
      return keys[0];
    case 2:
      return Fun.t2(keys[0], keys[1]);
    case 3:
      return Fun.t3(keys[0], keys[1], keys[2]);
    case 4:
      return Fun.t4(keys[0], keys[1], keys[2], keys[3]);
    case 5:
      return Fun.t5(keys[0], keys[1], keys[2], keys[3], keys[4]);
    case 6:
      return Fun.t6(keys[0], keys[1], keys[2], keys[3], keys[4], keys[5]);
    default:
      throw new ClassCastException("The key field number is not supported:"+fields.length);
    }
  }

  public Record transValue(Record record){
    Record res = new Record(valueSchema);
    for(int i = 0; i < valueFields.length; i++){
      res.put(i, record.get(valueFields[i]));
    }
    return res;
  }

  public void commit(){
    if(isBtree){
      db.commit();
      index = 0;
    }
  }

  public void close(){
    if(isBtree){
      db.commit();
      db.close();
    }
  }
}
