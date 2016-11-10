package org.apache.trevni.avro.update;

import static org.apache.trevni.avro.update.AvroColumnator.isSimple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.trevni.TrevniRuntimeException;
import org.apache.trevni.update.ColumnValues;
import org.apache.trevni.update.FileColumnMetaData;
import org.apache.trevni.update.InsertColumnFileReader;

public class ColumnReader<D> implements Closeable {
  InsertColumnFileReader reader;
  private GenericData model;
  private ColumnValues[] values;
  private int[] readNO;
  private int[] arrayWidths;
  private int column;

  public ColumnReader(File file) throws IOException{
    this(file, GenericData.get());
  }

  public ColumnReader(File file, GenericData model) throws IOException{
    this.reader = new InsertColumnFileReader(file);
    this.model = model;
    this.values = new ColumnValues[reader.getColumnCount()];
    for(int i = 0; i < values.length; i++){
      values[i] = reader.getValues(i);
    }
  }

  public D search(Schema readSchema, long row) {
    AvroColumnator readColumnator = new AvroColumnator(readSchema);
    FileColumnMetaData[] readColumns = readColumnator.getColumns();
    arrayWidths = readColumnator.getArrayWidths();
    readNO = new int[readColumns.length];
    for(int i = 0; i < readColumns.length; i++){
      readNO[i] = reader.getColumnNumber(readColumns[i].getName());
    }
    try{
    column = 0;
    return (D)read(readSchema, row);
    }catch(IOException e){
      throw new TrevniRuntimeException(e);
    }
  }

  private Object read(Schema s, long row) throws IOException{
    if(isSimple(s)){
      return readValue(s, column++, row);
    }
    final int startColumn = column;

    switch (s.getType()) {
    case RECORD:
      Object record = model.newRecord(null, s);
      for(Field f : s.getFields()) {
        Object value = read(f.schema(), row);
        model.setField(record, f.name(), f.pos(), value);
      }
      return record;
    case ARRAY:
      long newRow = 0;
      values[readNO[column]].startBlock(0);
      for(int i = 0; i < row; i++){
        newRow += values[readNO[column]].nextLength();
        values[readNO[column]].startRow();
      }
      int length = values[readNO[column]].nextLength();
      List elements = (List) new GenericData.Array(length, s);
      for(int i = 0; i < length; i++){
        this.column = startColumn;
        Object value;
        if(isSimple(s.getElementType()))
          value = readValue(s, ++column, (newRow + i));
        else {
          column++;
          value = read(s.getElementType(), (newRow + i));
        }
        elements.add(value);
      }
      column = startColumn + arrayWidths[startColumn];
      return elements;
    default:
      throw new TrevniRuntimeException("Unknown schema: " + s);
    }
  }

  private Object readValue(Schema s, int column, long row) throws IOException {
    values[readNO[column]].seek(row);
    Object v = values[readNO[column]].nextValue();
    values[readNO[column]].startRow();

    switch (s.getType()) {
    case ENUM:
      return model.createEnum(s.getEnumSymbols().get((Integer)v), s);
    case FIXED:
      return model.createFixed(null, ((ByteBuffer)v).array(), s);
    }

    return v;
  }

  @Override
  public void close() throws IOException{
    reader.close();
  }
}
