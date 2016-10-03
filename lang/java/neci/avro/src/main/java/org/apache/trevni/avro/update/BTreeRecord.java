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
  private static int index = 0;
  private static final int MAX = 200000;
  private DB db;
  private BTreeMap mTree;

  public BTreeRecord(int[] keyFields, int[] valueFields, Schema schema, File file, String name){
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    this.schema = schema;
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
    this.dbFile = file;
    this.name = name;
    db = DBMaker.newFileDB(dbFile).make();
  }

  public void create(){
    mTree = db.getTreeMap(name);
  }

  public void put(Record record){
    Object key = transKey(record);
    mTree.put(key, transValue(record).toString());
    index++;
    if(index == MAX){
      db.commit();
      index = 0;
    }
  }

  public void remove(Record record){
    Object key = transKey(record);
    mTree.remove(key);
  }

  public Object get(Record record){
    Object key = transKey(record);
    return mTree.get(key);
  }

  public BTreeKeySerializer transSer(){
    Serializer[] sers = new Serializer[keyFields.length];
    List<Field> fs = schema.getFields();
    for(int i = 0; i < sers.length; i++){
      Type type = fs.get(keyFields[i]).schema().getType();
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
    switch(keyFields.length){
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
      throw new ClassCastException("The key field number is not supported:"+keyFields.length);
    }
  }

  public Object transKey(Record record){
    Object[] keys = new Object[keyFields.length];
    List<Field> fs = schema.getFields();
    for(int i = 0; i < keyFields.length; i++){
      Type type = fs.get(keyFields[i]).schema().getType();
      switch(type){
      case BOOLEAN:
        keys[i] = Boolean.parseBoolean(record.get(keyFields[i]).toString());
        break;
      case INT:
        keys[i] = Integer.parseInt(record.get(keyFields[i]).toString());
        break;
      case LONG:
        keys[i] = Long.parseLong(record.get(keyFields[i]).toString());
        break;
      case STRING:
        keys[i] = record.get(keyFields[i]).toString();
        break;
      case BYTES:
        keys[i] = record.get(keyFields[i]).toString().getBytes();
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

  public Record transValue(Record record){
    Record res = new Record(valueSchema);
    for(int i = 0; i < valueFields.length; i++){
      res.put(i, record.get(valueFields[i]));
    }
    return res;
  }

  public void close(){
    db.commit();
    db.close();
  }

  public BTreeCreator createBTree(){
    return new BTreeCreator();
  }

  public class BTreeCreator{
    private BTreeMap tree;
    private int index = 0;

    public BTreeCreator(){
    create();
    }

    private void create(){
      BTreeKeySerializer ser = transSer();
      tree = db.createTreeMap(name).keySerializer(ser).valueSerializer(Serializer.STRING).make();
    }

    public void add(Record record){
      Object key = transKey(record);
      tree.put(key, transValue(record).toString());
      index++;
      if(index == MAX){
        db.commit();
        index = 0;
      }
    }

    public void commit(){
      db.commit();
    }
  }
}
