package org.apache.trevni.avro.update;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

public class NestSchema{
  private Schema schema;
  private Schema nestedSchema;
  private Schema encodeSchema;
  private Schema encodeNestedSchema;
  private int[] keyFields;
  private int[] outKeyFields;
  private File prFile;
  private String path;
  private File bloomFile;
  private File btreeFile;

  public NestSchema(Schema schema, int[] keyFields){
    this(schema, keyFields, null);
  }

  public NestSchema(Schema schema, int[] keyFields, int[] outKeyFields){
    this.schema = schema;
    this.keyFields = keyFields;
    this.outKeyFields = outKeyFields;
    encodeSchema = encode('a', schema);
  }

  public Schema encode(char s, Schema schema){
    List<Field> fields = new ArrayList<Field>();
    int i = 1;
    assert(schema.getType().compareTo(Type.RECORD) == 0);
    for(Field f : schema.getFields()){
      if(f.schema().getType().compareTo(Type.ARRAY) == 0){
        Schema tmp = encode((char)(s + 1), f.schema().getElementType());
        fields.add(new Schema.Field(((char)(s + 1) + "A"), Schema.createArray(tmp), null, null));
        i++;
      }else{
        fields.add(new Schema.Field((s + String.valueOf(i)), f.schema(), null, null));
        i++;
      }
    }
    return Schema.createRecord(String.valueOf(s), null, null, false, fields);
  }

  public Schema getEncodeSchema(){
    return encodeSchema;
  }

  public Schema getEncodeNestedSchema(){
    return encodeNestedSchema;
  }

  public void setBloomFile(File bloomFile){
    this.bloomFile = bloomFile;
  }
  public File getBloomFile(){
    return bloomFile;
  }

  public void setBTreeFile(File btreeFile){
    this.btreeFile = btreeFile;
  }
  public File getBTreeFile(){
    return btreeFile;
  }

  public void setPath(String path){
    this.path = path;
  }
  public void setPrFile(File prFile){
    this.prFile = prFile;
  }
  public void setSchema(Schema schema){
    this.schema = schema;
  }
  public void setNestedSchema(Schema nestedSchema){
    this.nestedSchema = nestedSchema;
    encodeNestedSchema = encode('a', nestedSchema);
  }
  public void setKeyFields(int[] keyFields){
    this.keyFields = keyFields;
  }
  public void setOutKeyFields(int[] outKeyFields){
    this.outKeyFields = outKeyFields;
  }

  public String getPath(){
    return path;
  }
  public File getPrFile(){
    return prFile;
  }
  public Schema getSchema(){
    return schema;
  }
  public Schema getNestedSchema(){
    if(nestedSchema == null){
      return schema;
    }else{
      return nestedSchema;
    }
  }
  public int[] getKeyFields(){
    return keyFields;
  }
  public int[] getOutKeyFields(){
    return outKeyFields;
  }
}
