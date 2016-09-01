package org.apache.trevni.avro.update;

import java.io.File;

import org.apache.avro.Schema;

public class NestSchema{
  private Schema schema;
  private int[] keyFields;
  private int[] outKeyFields;
  private File file;
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

  public void setFile(File file){
    this.file = file;
  }
  public void setSchema(Schema schema){
    this.schema = schema;
  }
  public void setKeyFields(int[] keyFields){
    this.keyFields = keyFields;
  }
  public void setOutKeyFields(int[] outKeyFields){
    this.outKeyFields = outKeyFields;
  }

  public File getFile(){
    return file;
  }
  public Schema getSchema(){
    return schema;
  }
  public int[] getKeyFields(){
    return keyFields;
  }
  public int[] getOutKeyFields(){
    return outKeyFields;
  }
}
