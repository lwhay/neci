package org.apache.trevni.avro.update;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Record;

public class KeyToBytes {
  private static Schema schema;
  private Record record;
  private int[] keyFields;
  private int length;
  private byte[][] bytes;

  public KeyToBytes(Record record){
    this.record = record;
    schema = record.getSchema();
    create();
  }

  public KeyToBytes(Record record, int[] keyFields){
    this.record = record;
    schema = record.getSchema();
    this.keyFields = keyFields;
    create();
  }

  public void create(){
    int i = 0;
    List<Field> fs = schema.getFields();
    if(keyFields == null){
      bytes = new byte[fs.size()][];
      for(Field f : fs){
        switch(f.schema().getType()){
        case LONG:  {  createLongField(Long.parseLong(record.get(i).toString()), i); i++;  break;  }
        case INT:  {  createIntField(Integer.parseInt(record.get(i).toString()), i); i++;  break;  }
        case FLOAT:  {  int v = Float.floatToIntBits(Float.parseFloat(record.get(i).toString()));  createIntField(v, i); i++;  break;  }
        case DOUBLE: {  long v = Double.doubleToLongBits(Long.parseLong(record.get(i).toString()));  createLongField(v, i); i++;  break;  }
        case BYTES:
        case STRING:  {  createStringField(record.get(i).toString(), i); i++;  break;  }
        default:  throw new RuntimeException("This Key type is not support!");
        }
      }
    }else{
      bytes = new byte[keyFields.length][];
      for(i = 0; i < keyFields.length; i++){
        Field f = fs.get(keyFields[i]);
        switch(f.schema().getType()){
        case LONG:  {  createLongField(Long.parseLong(record.get(keyFields[i]).toString()), i);  break;  }
        case INT:  {  createIntField(Integer.parseInt(record.get(keyFields[i]).toString()), i);  break;  }
        case FLOAT:  {  int v = Float.floatToIntBits(Float.parseFloat(record.get(keyFields[i]).toString()));  createIntField(v, i);  break;  }
        case DOUBLE: {  long v = Double.doubleToLongBits(Long.parseLong(record.get(keyFields[i]).toString()));  createLongField(v, i);  break;  }
        case BYTES:
        case STRING:  {  createStringField(record.get(keyFields[i]).toString(), i);  break;  }
        default:  throw new RuntimeException("This Key type is not support!");
        }
      }
    }
  }

  private void createLongField(long v, int i){
    byte[] dest = new byte[8];
    dest[0] = (byte)(v & 0xFF);
    for(int k = 1; k < 8; k++){
      dest[k] = (byte)((v >> 8) & 0xFF);
      v = v >> 8;
    }
    length += 8;
    bytes[i] = new byte[8];
    bytes[i] = dest;
  }

  private void createIntField(int v, int i){
    byte[] dest = new byte[4];
    dest[0] = (byte)(v & 0xFF);
    for(int k = 1; k < 4; k++){
        dest[k] = (byte)((v >> 8) & 0xFF);
        v = v >> 8;
      }
    length += 4;
    bytes[i] = new byte[4];
    bytes[i] = dest;
  }

  private void createStringField(String v, int i){
    byte[] dest = record.get(i).toString().getBytes();
    length += dest.length;
    bytes[i] = new byte[dest.length];
    bytes[i] = dest;
  }

  public byte[][] getBytes(){
    return bytes;
  }
  public byte[] getFieldBytes(int i){
    return bytes[i];
  }

  public int getLength(){
    return length;
  }
  public int getFieldLength(int i){
    return bytes[i].length;
  }
}
