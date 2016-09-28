package org.apache.trevni.update;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.trevni.ValueType;

public class InsertColumnFileWriter {
  private FileColumnMetaData[] meta;
  private FileMetaData filemeta;
  private InsertColumnFileReader reader;
  private ColumnValues[] values;
  private ListArr[] insert;
  private long rowcount;
  private int columncount;
  private long[] gap;
  private int[] nest;
  private long[] columnStart;
  private Blocks[] blocks;

  private int addRow;
  static final byte[] MAGIC = new byte[] { 'T', 'r', 'v', 2 };

  class Blocks{
    private List<BlockDescriptor> blocks;

    Blocks(){
      blocks = new ArrayList<BlockDescriptor>();
    }

    void add(BlockDescriptor b){
      blocks.add(b);
    }

    void clear(){
      blocks.clear();
    }

    BlockDescriptor get(int i){
      return blocks.get(i);
    }
  }

  public static class ListArr {
    private List<Object> x;

    public ListArr() {
      this.x = new ArrayList<Object>();
    }

    public void add(Object o) {
      this.x.add(o);
    }

    public void clear() {
      x.clear();
    }

    public Object get(int i){
      return x.get(i);
    }

    public Object[] toArray(){
      return x.toArray();
    }

    public int size(){
      return x.size();
    }
  }

   // public static void MemPrint(){
   //     System.out.println("$$$$$$$$$\t"+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
   // }

//  public InsertColumnFileWriter(File fromfile, ListArr[] sort) throws IOException {
//    this.reader = new InsertColumnFileReader(fromfile);
//    this.insert = sort;
//    this.filemeta = reader.getMetaData();
//    this.meta = reader.getFileColumnMetaData();
//    this.addRow = sort[0].size();
//  }

  public InsertColumnFileWriter(FileMetaData filemeta, FileColumnMetaData[] meta)
    throws IOException {
    this.filemeta = filemeta;
    this.meta = meta;
    this.columncount = meta.length;
    this.columnStart = new long[columncount];
    this.blocks = new Blocks[columncount];
    for(int i = 0; i < columncount; i++){
      blocks[i] = new Blocks();
    }
  }

  public void setReadFile(File file) throws IOException{
    this.reader  = new InsertColumnFileReader(file);
  }

  public void setInsert(ListArr[] sort){
    this.insert = sort;
    this.addRow = sort[0].size();
  }

  public void setGap(long[] gap){
    this.gap = gap;
  }

  public void appendTo(File file) throws IOException {
    OutputStream data = new FileOutputStream(file);
    OutputStream head = new FileOutputStream(new File(file.getPath().substring(0, file.getPath().lastIndexOf(".")) + ".head"));
    appendTo(head, data);
  }

  public void insertTo(File file) throws IOException {
    OutputStream data = new FileOutputStream(file);
    OutputStream head = new FileOutputStream(new File(file.getPath().substring(0, file.getPath().lastIndexOf(".")) + ".head"));
    insertTo(head, data);
  }

  public void appendTo(OutputStream head, OutputStream data) throws IOException {
    rowcount = addRow;

    writeSourceColumns(data);
    writeHeader(head);
  }

  public void insertTo(OutputStream head, OutputStream data) throws IOException {
    rowcount = addRow + reader.getRowCount();
    values = new ColumnValues[meta.length];
    for(int i = 0; i < meta.length; i++){
      values[i] = reader.getValues(i);
    }

    writeColumns(data);
    writeHeader(head);
  }

  private void writeSourceColumns(OutputStream out) throws IOException {
    OutputBuffer buf = new OutputBuffer();
    for (int i = 0; i < columncount; i++) {
      ValueType type = meta[i].getType();
      int row = 0;
      if (type == ValueType.NULL) {
        for (Object x : insert[i].toArray()){
          if(buf.isFull()){
            BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
            blocks[i].add(b);
            row = 0;
            buf.writeTo(out);
            buf.reset();
          }
          buf.writeLength((Integer) x);
          row++;
        }
      } else {
        for (Object x : insert[i].toArray()){
          if(buf.isFull()){
            BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
            blocks[i].add(b);
            row = 0;
            buf.writeTo(out);
            buf.reset();
          }
          buf.writeValue(x, type);
          row++;
        }
      }

      insert[i].clear();

      if(buf.size() != 0){
        BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
        blocks[i].add(b);
        buf.writeTo(out);
        buf.reset();
      }
    }
    insert = null;
    buf.close();
  }

  private void writeColumns(OutputStream out) throws IOException {
    assert(gap.length == (addRow + 1));
    nest = new int[addRow];
    for (int k = 0; k < addRow; k++) {
      nest[k] = 1;
    }

    for (int j = 0; j < columncount; j++) {
      if (meta[j].getType() == ValueType.NULL) {
        writeArrayColumn(out, j);
      } else {
        writeColumn(out, j);
      }
    }
//        MemPrint();
    reader.close();
    reader = null;
    values = null;
    insert = null;
    gap = null;
    nest = null;
  }

  private void writeColumn(OutputStream out, int column) throws IOException {
    OutputBuffer buf = new OutputBuffer();
    int row = 0;
    int realrow = 0;
    ValueType type = meta[column].getType();

    for (int i = 0; i < addRow; i++) {
      for (int k = 0; k < gap[i]; k++) {
        if(buf.isFull()){
          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
          blocks[column].add(b);
          row = 0;
          buf.writeTo(out);
          buf.reset();
        }
        values[column].startRow();
        buf.writeValue(values[column].nextValue(), type);
        row++;
      }
      for (int r = 0; r < nest[i]; r++) {
        if(buf.isFull()){
          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
          blocks[column].add(b);
          row = 0;
          buf.writeTo(out);
          buf.reset();
        }
        buf.writeValue(insert[column].get(realrow), type);
        row++;
        realrow++;
      }
    }
    assert (realrow == insert[column].size());
    insert[column].clear();

    for (int k = 0; k < gap[addRow]; k++) {
      if(buf.isFull()){
        BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
        blocks[column].add(b);
        row = 0;
        buf.writeTo(out);
        buf.reset();
      }
      values[column].startRow();
      buf.writeValue(values[column].nextValue(), type);
      row++;
    }

    if(buf.size() != 0){
      BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
      blocks[column].add(b);
      buf.writeTo(out);
    }

    buf.close();
  }


  private void writeArrayColumn(OutputStream out, int column) throws IOException {
    OutputBuffer buf = new OutputBuffer();
    int row = 0;
    int realrow = 0;
    long[] tmgap = new long[addRow + 1];
    int[] tmnest = new int[addRow];
    ValueType type = meta[column].getType();
    if(type == ValueType.NULL){
      int y = 0;
      for(int x = 0; x <addRow; x++){
        for(int no = 0; no < nest[x]; no++){
          tmnest[x] += (Integer) insert[column].get(y + no);
        }
        y += nest[x];
      }
    }

    for (int i = 0; i < addRow; i++) {
      for (long k = 0; k < gap[i]; k++) {
        if(buf.isFull()){
          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
          blocks[column].add(b);
          row = 0;
          buf.writeTo(out);
          buf.reset();
        }
        values[column].startRow();
        int length = values[column].nextLength();
        buf.writeLength(length);
        tmgap[i] += length;
        row++;
      }
      for (int r = 0; r < nest[i]; r++) {
        if(buf.isFull()){
          BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
          blocks[column].add(b);
          row = 0;
          buf.writeTo(out);
          buf.reset();
        }
        buf.writeLength((Integer) insert[column].get(realrow));
        realrow++;
        row++;
      }
    }
    assert (realrow == insert[column].size());
    insert[column].clear();

    for (long k = 0; k < gap[addRow]; k++) {
      if(buf.isFull()){
        BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
        blocks[column].add(b);
        row = 0;
        buf.writeTo(out);
        buf.reset();
      }
      values[column].startRow();
      int len = values[column].nextLength();
      buf.writeLength(len);
      tmgap[addRow] += len;
      row++;
    }
    nest = tmnest;
    gap = tmgap;

    if(buf.size() != 0){
      BlockDescriptor b = new BlockDescriptor(row, buf.size(), buf.size());
      blocks[column].add(b);
      buf.writeTo(out);
    }

    buf.close();
  }

  public void writeHeader(OutputStream out) throws IOException {
    OutputBuffer header = new OutputBuffer();
    header.write(MAGIC);
    header.writeFixed64(rowcount);
    header.writeFixed32(columncount);
    filemeta.write(header);
    int i = 0;
    long delay = 0;
    for (FileColumnMetaData c : meta) {
      columnStart[i] = delay;
      c.write(header);
      int size = blocks[i].blocks.size();
      header.writeFixed32(size);
      for(int k = 0; k < size; k++){
        blocks[i].get(k).writeTo(header);
        delay += blocks[i].get(k).compressedSize;
      }
      blocks[i].clear();
      i++;
    }

    for(i = 0; i < columncount; i++){
      header.writeFixed64(columnStart[i]);
    }
    header.writeTo(out);
    header.close();
  }
}
