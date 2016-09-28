package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;

import org.apache.trevni.ValueType;
import org.apache.trevni.update.ColumnValues;
import org.apache.trevni.update.InsertColumnFileReader;

public class SortTrevniReader{
  private InsertColumnFileReader reader;
  private ColumnValues[] values;
  private ValueType[] types;

  public SortTrevniReader(File file, int[] keyFields) throws IOException{
    reader = new InsertColumnFileReader(file);
    values = new ColumnValues[keyFields.length];
    types = new ValueType[keyFields.length];
    for(int i = 0; i < keyFields.length; i++){
      values[i] = reader.getValues(i);
      types[i] = values[i].getType();
    }
  }

  public ValueType[] getTypes(){
    return types;
  }

  public CombKey nextKey(){
    Object[] keys = new Object[values.length];
    for(int i = 0; i < values.length; i++){
      keys[i] = values[i].next();
    }
    return new CombKey(keys, types);
  }

  public boolean hasNextKey(){
    return values[0].hasNext();
  }

  public void close() throws IOException{
    reader.close();
  }
}
