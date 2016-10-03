package org.apache.trevni.avro.update;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.trevni.ValueType;
import org.apache.avro.generic.GenericData.Record;

public class CombKey implements Comparable<CombKey>{
  private Object[] keys;
  private boolean[] types;

  public CombKey(int len){
    keys = new Object[len];
    types = new boolean[len];
  }

  public CombKey(Record record){
    List<Field> fs = record.getSchema().getFields();
    int len = fs.size();
    this.keys = new Object[len];
    this.types = new boolean[len];
    for(int i = 0; i < len; i++){
      types[i] = isInteger(fs.get(i));
      keys[i] = record.get(i);
    }
  }

  public CombKey(Record record, int[] keyFields){
    int len = keyFields.length;
    this.keys = new Object[len];
    this.types = new boolean[len];
    List<Field> fs = record.getSchema().getFields();
    for(int i = 0; i < len; i++){
      types[i] = isInteger(fs.get(keyFields[i]));
      keys[i] = record.get(keyFields[i]);
    }
  }

  public CombKey(Object[] keys, ValueType[] tt){
    assert(keys.length == tt.length);
    this.keys = keys;
    this.types = new boolean[keys.length];
    for(int i = 0; i < keys.length; i++){
      types[i] = isInteger(tt[i]);
    }
  }

  boolean isInteger(Field f){
    switch(f.schema().getType()){
    case LONG:
    case INT:
      return true;
    case STRING:
    case BYTES:
      return false;
    default:  throw new ClassCastException("This type is not supported for Key type: " + f.schema());
    }
  }

  boolean isInteger(ValueType type){
    switch(type){
      case LONG:
      case INT:
        return true;
      case STRING:
      case BYTES:
        return false;
      default:  throw new ClassCastException("This type is not supported for Key type: " + type);
    }
  }

  public CombKey(Object[] keys, boolean[] types){
    assert(keys.length == types.length);
    this.keys = keys;
    this.types = types;
  }

  @Override
  public int compareTo(CombKey o){
    assert(this.getLength() == o.getLength());
    assert(types == o.types);
    for(int i = 0; i < getLength(); i++){
      if(types[i]){
        long k1 = Long.parseLong(keys[i].toString());
        long k2 = Long.parseLong(o.keys[i].toString());
        if(k1 > k2)
          return 1;
        else if(k1 < k2)
          return -1;
      }else{
        String k1 = keys[i].toString();
        String k2 = o.keys[i].toString();
        if(k1.compareTo(k2) > 0)
          return 1;
        else if(k1.compareTo(k2) < 0)
          return -1;
      }
    }
    return 0;
  }

  public int getLength(){
   return keys.length;
  }

  public Object[] get(){
    return keys;
  }
}
