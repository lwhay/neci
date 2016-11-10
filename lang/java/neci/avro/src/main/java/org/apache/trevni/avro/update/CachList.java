package org.apache.trevni.avro.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;

public class CachList {
  private List<FlagData> cach;
  private int[] keyFields;
  private int size;
  private HashMap<ComparableKey, Integer> hash;
  private static int max;
  public CachList(int[] keyFields){
    cach = new ArrayList<FlagData>();
    this.keyFields = keyFields;
    hash = new HashMap<ComparableKey, Integer>();
  }
  public void setMAX(int max){
    this.max = max;
  }
  public void add(Record data, byte flag){
    add(new FlagData(flag, data));
  }
  public void add(FlagData fd){
    hash.put(new ComparableKey(fd.getData(), keyFields), cach.size());
    cach.add(fd);
    size++;
  }
  public void delete(Record data){
    delete(new ComparableKey(data, keyFields));
  }
  public void delete(ComparableKey key){
    Object v = hash.get(key);
    if(v != null){
//      cach.remove((int)v);
      hash.remove(key);
      size--;
    }
  }
  public byte find(Record data){
    return find(new ComparableKey(data, keyFields));
  }
  public Object getIndex(ComparableKey key){
    return hash.get(key);
  }
  public byte find(ComparableKey key){
    Object v = hash.get(key);
    if(v != null)
      return cach.get((int)v).getFlag();
    else return -1;
  }
  public int size(){
    return size;
  }
  public boolean isFull(){
    return (cach.size() >= max);
  }

  public class FlagData{
    private byte flag;
    private Record data;
    public FlagData(byte flag, Record data){
      this.flag = flag;
      this.data = data;
    }
    public void setFlag(byte flag){
      this.flag = flag;
    }
    public void setData(Record data){
      this.data = data;
    }
    public byte getFlag(){
      return flag;
    }
    public Record getData(){
      return data;
    }
    public String toString(){
      return flag + "|" + data.toString();
    }
  }
}
