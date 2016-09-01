package org.apache.trevni.avro.update;

import static org.apache.trevni.avro.update.AvroColumnator.isSimple;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.util.Utf8;
import org.apache.trevni.update.FileColumnMetaData;
import org.apache.trevni.update.FileMetaData;
import org.apache.trevni.update.InsertColumnFileWriter;
import org.apache.trevni.update.InsertColumnFileWriter.ListArr;
import org.apache.trevni.TrevniRuntimeException;

public class InsertAvroColumnWriter<K, V> {
  private Schema schema;
  private InsertColumnFileWriter writer;
  private FileColumnMetaData[] meta;
  private FileMetaData filemeta;
  private MyMap<K, V> sort;
  private ListArr[] v;
  private File file0, file1;
  private int[] arrayWidths;
  private GenericData model;
  boolean changefile = false;
  int x = 0;
  long start, end;

  public static final String SCHEMA_KEY = "avro.schema";
  private static final int MAX = 200000;

  public InsertAvroColumnWriter(Schema schema, String file0, String file1) throws IOException {
    this.schema = schema;
    AvroColumnator columnator = new AvroColumnator(schema);
    filemeta = new FileMetaData();
    filemeta.set(SCHEMA_KEY, schema.toString());
    this.meta = columnator.getColumns();
    this.writer = new InsertColumnFileWriter(filemeta, meta);
    this.arrayWidths = columnator.getArrayWidths();
    this.model = GenericData.get();
    //this.keyfields = keyfields;
    this.file0 = new File(file0);
    this.file1 = new File(file1);
    sort = new MyMap<K, V>();
    v = new ListArr[meta.length];
    for (int k = 0; k < v.length; k++) {
        v[k] = new ListArr();
      }
    start = System.currentTimeMillis();
  }

  //public static void MemPrint(){
  //System.out.println("*********\t"+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
  //}

  public void append(K key, V value) throws IOException {
    sort.put(key, value);
    if (sort.size() == MAX) {
      if (changefile) {
        appendTo(file0, file1);
        changefile = !changefile;
      } else {
        appendTo(file1, file0);
        changefile = !changefile;
      }
      end = System.currentTimeMillis();
      x++;
      System.out.println("############" + x+"\ttime: "+(end - start)+"ms");
      start = System.currentTimeMillis();
    }
  }

  public void flush() throws IOException {
    if (!sort.isEmpty()) {
      x++;
      if (changefile) {
        appendTo(file0, file1);
        changefile = !changefile;
      } else {
        appendTo(file1, file0);
        changefile = !changefile;
      }
      end = System.currentTimeMillis();
      System.out.println("############" + x+"\ttime: "+(end - start)+"ms");
    }
  }

  private int append(Object o, Schema s, int column) throws IOException {
    if (isSimple(s)) {
      appendValue(o, s, column);
      return column + 1;
    }
    switch (s.getType()) {
      case RECORD:
        for (Field f : s.getFields())
           column = append(model.getField(o, f.name(), f.pos()), f.schema(), column);
           return column;
      case ARRAY:
        Collection elements = (Collection) o;
        appendValue(elements.size(), s, column);
        if (isSimple(s.getElementType())) { // optimize simple arrays
          column++;
          for (Object element : elements)
            appendValue(element, s.getElementType(), column);
          return column + 1;
        }
        for (Object element : elements) {
          int c = append(element, s.getElementType(), column + 1);
          assert (c == column + arrayWidths[column]);
        }
        return column + arrayWidths[column];
      default:
        throw new TrevniRuntimeException("Unknown schema: " + s);
    }
  }

  private void appendValue(Object o, Schema s, int column) throws IOException {
    switch(s.getType()){
      case STRING:
        if(o instanceof Utf8)
          o = o.toString();
        break;
      case ENUM:
        if(o instanceof Enum)
          o = ((Enum)o).ordinal();
        else
          o = s.getEnumOrdinal(o.toString());
        break;
      case FIXED:
        o = ((GenericFixed) o).bytes();
        break;
    }
    v[column].add(o);
  }

  public void appendTo(File fromfile, File tofile) throws IOException {
    tofile.delete();
    List<V> values = sort.values();
    sort.clear();
    for (int i = values.size(); i > 0; i--) {
      V record = values.get(i - 1);
      int count = append(record, schema, 0);
      assert (count == meta.length);
    }
    values = null;
    writer.setInsert(v);
    v = null;
    v = new ListArr[meta.length];
    for (int k = 0; k < v.length; k++) {
      v[k] = new ListArr();
    }
    if (!fromfile.exists()) {
      writer.appendTo(tofile);
    } else {
      System.out.println("!!!!!!!!merge file size: "+fromfile.length());
      writer.setReadFile(fromfile);
      writer.insertTo(tofile);
    }
  }
}
