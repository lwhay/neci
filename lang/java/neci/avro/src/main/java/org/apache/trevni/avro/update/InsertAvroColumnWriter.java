package org.apache.trevni.avro.update;

import static org.apache.trevni.avro.update.AvroColumnator.isSimple;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

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
  private int[] keyFields;
  private InsertColumnFileWriter writer;
  private FileColumnMetaData[] meta;
  private FileMetaData filemeta;
  private SortedArray<K, V> sort;
  private ListArr[] v;
  private String path;
//  private File[] files;
//  private File tmpFile;
//  private int numFiles;
  private int[] arrayWidths;
  private GenericData model;
  private long bytes;
  int x = 0;
  long start, end;

  private int fileIndex = 0;

  public static final String SCHEMA_KEY = "avro.schema";
  private int max;
  private int free;
  private int mul;

  public InsertAvroColumnWriter(Schema schema, String path, int[] keyFields, int free, int mul) throws IOException {
    this.schema = schema;
    AvroColumnator columnator = new AvroColumnator(schema);
    filemeta = new FileMetaData();
    filemeta.set(SCHEMA_KEY, schema.toString());
    this.meta = columnator.getColumns();
    this.writer = new InsertColumnFileWriter(filemeta, meta);
    this.arrayWidths = columnator.getArrayWidths();
    this.model = GenericData.get();
    this.keyFields = keyFields;
//    this.numFiles = numFiles;
    this.free = free;
    this.mul = mul;
//    fileDelete(path);
    this.path = path;
    bytes = 0;
//    createFiles(path, numFiles);
    sort = new SortedArray<K, V>();
    v = new ListArr[meta.length];
    for (int k = 0; k < v.length; k++) {
        v[k] = new ListArr();
      }
    start = System.currentTimeMillis();
  }

  public void fileDelete(String path){
    File file = new File(path);
    if(file.exists() & file.isDirectory()){
      File[] files = file.listFiles();
      for(int i = 0; i < files.length; i++){
        files[i].delete();
      }
    }
    if(!file.exists()){
      file.mkdirs();
    }
  }

//  public void createFiles(String path, int no){
//    files = new File[no];
//    for(int i = 0; i < no; i++){
//      files[i] = new File(path+"file"+String.valueOf(i)+".trv");
//    }
//    tmpFile = new File(path + "tmp.trv");
//  }

  //public static void MemPrint(){
  //System.out.println("*********\t"+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
  //}

  public void append(K key, V value) throws IOException {
    sort.put(key, value);
    if(x == 0){
      bytes += value.toString().length();
      if (Runtime.getRuntime().freeMemory() <= (free * 1024 * 1024)) {
        max = sort.size();
        mul = max / mul;
        System.out.println("####sortarray max####" + mul);
        System.out.println("####max####" + max);
        System.out.println("&&&&&&bytes&&&&\t" + bytes);
      }else{
        return;
      }
    }else{
      if(sort.size() < max){
        return;
      }
    }
//    if(!files[fileIndex].exists()){
//    if(sort.size() == 500){
      appendTo(new File(path+"file"+String.valueOf(fileIndex)+".trv"));

//    }else{
//      appendTo(files[fileIndex], tmpFile);
//      File headFile = new File(files[fileIndex].getPath().substring(0, files[fileIndex].getPath().lastIndexOf(".")) + ".head");
//      File tmpHeadFile = new File(tmpFile.getPath().substring(0, tmpFile.getPath().lastIndexOf(".")) + ".head");
//      files[fileIndex].delete();
//      headFile.delete();
//      tmpFile.renameTo(files[fileIndex]);
//      tmpHeadFile.renameTo(headFile);
//    }
    fileIndex++;
    end = System.currentTimeMillis();
    System.out.println("############" + (++x)+"\ttime: "+(end - start)+"ms");
    System.out.println();
    start = System.currentTimeMillis();
//    }
  }

  public int flush() throws IOException {
    if (!sort.isEmpty()) {
      appendTo(new File(path+"file"+String.valueOf(fileIndex)+".trv"));
      fileIndex++;
      end = System.currentTimeMillis();
      System.out.println("Trevni#######" + (++x)+"\ttime: "+(end - start)+"ms");
    }
    System.gc();
    return fileIndex;
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

//  public void appendTo(File fromfile, File tofile) throws IOException {
//    for (V record : sort.values(mul)) {
//      int count = append(record, schema, 0);
//      assert (count == meta.length);
//    }
//    writer.setInsert(v);
//    writer.setGap(compGap(fromfile));
//    v = null;
//    v = new ListArr[meta.length];
//    for (int k = 0; k < v.length; k++) {
//      v[k] = new ListArr();
//    }
//    long t1 = System.currentTimeMillis();
//    writer.setReadFile(fromfile);
//    writer.insertTo(tofile);
//    long t2 = System.currentTimeMillis();
//    System.out.println("@@@write time:  " + (t2 - t1));
//  }

  public void appendTo(File file) throws IOException{
    while(sort.size() != 0){
      for (V record : sort.values(mul)) {
        int count = append(record, schema, 0);
        assert (count == meta.length);
      }
    }
    if(!file.getParentFile().exists()){
      file.getParentFile().mkdirs();
    }
    if(file.exists()){
      file.delete();
    }
    writer.setInsert(v);
    v = null;
    v = new ListArr[meta.length];
    for (int k = 0; k < v.length; k++) {
      v[k] = new ListArr();
    }
    long t1 = System.currentTimeMillis();
    writer.appendTo(file);
    long t2 = System.currentTimeMillis();
    System.out.println("@@@write time:  " + (t2 - t1));
  }

  public void mergeFiles(int[] gap, File[] files) throws IOException{
    long t1 = System.currentTimeMillis();
    writer.setGap(gap);
    writer.setMergeFiles(files);
    writer.mergeFiles(new File(path + "result.trv"));
    long t3 = System.currentTimeMillis();
    System.out.println("merge write time:\t" + (t3 - t1));
  }

//  public long[] compGap(File file) throws IOException{
//    SortTrevniReader reader = new SortTrevniReader(file, keyFields);
//    int addRow = v[0].size();
//    long[] gap = new long[addRow + 1];
//    int i = 0;
//    int g = 0;
//    while (reader.hasNextKey()) {
//      CombKey k0 = reader.nextKey();
//      while (i < addRow) {
//        if (k0.compareTo(new CombKey(getKeys(i), reader.getTypes())) > 0) {
//          gap[i] = g;
//          i++;
//          g = 0;
//          continue;
//        } else {
//          break;
//        }
//      }
//      g++;
//    }
//    gap[i] = g;
//
//    return gap;
//  }

  public  Object[] getKeys(int index){
    Object[] keys = new Object[keyFields.length];
    for(int i = 0; i < keyFields.length; i++){
      keys[i] = v[keyFields[i]].get(index);
    }
    return keys;
  }
}
