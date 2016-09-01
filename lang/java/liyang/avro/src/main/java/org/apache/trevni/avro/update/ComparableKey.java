package org.apache.trevni.avro.update;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;

public class ComparableKey implements Comparable<ComparableKey>{
  private Record record;
  private int[] keyFields;

  public ComparableKey(Record record, int[] keyFields){
    this.record = record;
    this.keyFields = keyFields;
  }

  @Override
  public int compareTo(ComparableKey o){
    if(keyFields.length != o.keyFields.length){
      throw new ClassCastException("This two classes are not matched!");
    }
    Schema s1 = record.getSchema();
    Schema s2 = o.record.getSchema();
    int[] k1 = keyFields;
    int[] k2 = o.keyFields;
    List<Field> f1 = s1.getFields();
    List<Field> f2 = s2.getFields();
    for(int i = 0; i < keyFields.length; i++){
      Type t1 = f1.get(k1[i]).schema().getType();
      Type t2 = f2.get(k2[i]).schema().getType();
      if(t1.compareTo(t2) != 0){
        throw new ClassCastException("This two classes are not matched!");
      }
      switch(t1){
      case STRING:
      case BYTES:  {
          int x = record.get(k1[i]).toString().compareTo(o.record.get(k2[i]).toString());
          if(x != 0)  return x;
          break;  }
      case INT:  {
          int m = Integer.parseInt(record.get(k1[i]).toString());
          int n = Integer.parseInt(o.record.get(k2[i]).toString());
          if(m > n)  return 1;
          else if(m < n)  return -1;
          else  break;  }
      case LONG:  {
          long m = Long.parseLong(record.get(k1[i]).toString());
          long n = Long.parseLong(o.record.get(k2[i]).toString());
          if(m > n)  return 1;
          else if(m < n)  return -1;
          else  break;  }
      case FLOAT:  {
          float m = Float.parseFloat(record.get(k1[i]).toString());
          float n = Float.parseFloat(o.record.get(k2[i]).toString());
          if(m > n)  return 1;
          else if(m < n)  return -1;
          else  break;  }
      case DOUBLE:  {
          double m = Double.parseDouble(record.get(k1[i]).toString());
          double n = Double.parseDouble(o.record.get(k2[i]).toString());
          if(m > n)  return 1;
          else if(m < n)  return -1;
          else  break;  }
      default:  throw new ClassCastException("The key field type is not supported!");
      }
    }
    return 0;
  }
}
