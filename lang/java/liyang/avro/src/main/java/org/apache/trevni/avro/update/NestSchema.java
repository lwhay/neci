package org.apache.trevni.avro.update;

import java.io.File;

import org.apache.avro.Schema;

public class NestSchema{
  private Schema schema;
  private Schema nestedSchema;
  private int[] keyFields;
  private int[] outKeyFields;
  private File prFile;
  private String path;
  private File bloomFile;

  public NestSchema(Schema schema, int[] keyFields){
    this(schema, keyFields, null);
  }

  public NestSchema(Schema schema, int[] keyFields, int[] outKeyFields){
    this.schema = schema;
    this.keyFields = keyFields;
    this.outKeyFields = outKeyFields;
  }

  public void setBloomFile(File bloomFile){
    this.bloomFile = bloomFile;
  }
  public File getBloomFile(){
    return bloomFile;
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
