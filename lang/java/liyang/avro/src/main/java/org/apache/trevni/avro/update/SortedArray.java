package org.apache.trevni.avro.update;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortedArray<K, V> {
  private Comparator<? super K> comparator;
  private boolean sorted;
  private List<Element> elements;

  private transient int size = 0;

  private class Element{
    private K key;
    private V value;

    Element(K key, V value){
      this.key = key;
      this.value = value;
    }

    K getKey(){
      return key;
    }
    V getValue(){
      return value;
    }

    void setKey(K key){
      this.key = key;
    }
    void setValue(V value){
      this.value = value;
    }
  }

  public SortedArray(){
    comparator = null;
    sorted = true;
    this.elements = new ArrayList<Element>();
  }

  public SortedArray(Comparator<? super K> comparator){
    this.comparator = comparator;
    sorted = true;
    this.elements = new ArrayList<Element>();
  }

  public V get(K key){
    if(!sorted)  sort();
    int i = 0;
    int j = size - 1;
    if(comparator != null){
      while(i != j){
        int m = (i + j) / 2;
        int cmp = comparator.compare(elements.get(m).getKey(), key);
        if(cmp < 0)  i = m;
        else if(cmp > 0)  j = m;
        else  return elements.get(m).getValue();
      }
    }else{
      if(key == null)
        throw new NullPointerException();
      while(i != j){
        int m = (i + j) / 2;
        int cmp = ((Comparable<? super K>)elements.get(m).getKey()).compareTo(key);
        if(cmp < 0)  i = m;
        else if(cmp > 0)  j = m;
        else  return elements.get(m).getValue();
          }
    }
    return null;
  }

  public int getNumber(K key){
    if(!sorted)  sort();
      int i = 0;
      int j = size - 1;
      if(comparator != null){
        while(i != j){
          int m = (i + j) / 2;
          int cmp = comparator.compare(elements.get(m).getKey(), key);
          if(cmp < 0)  i = m;
          else if(cmp > 0)  j = m;
          else  return m;
        }
      }else{
        if(key == null)
          throw new NullPointerException();
        while(i != j){
          int m = (i + j) / 2;
          int cmp = ((Comparable<? super K>)elements.get(m).getKey()).compareTo(key);
          if(cmp < 0)  i = m;
          else if(cmp > 0)  j = m;
          else  return m;
        }
      }
    return -1;
  }

  public Comparator<? super K> comparator(){
    return comparator;
  }

  public boolean containsKey(K key){
    return get(key) != null;
  }

  public boolean containsValue(V value){
    if(value == null)  throw new NullPointerException();
    for(int i = 0; i < size; i++){
      if(elements.get(i).getValue().equals(value))  return true;
    }
    return false;
  }

  public boolean isEmpty(){
    return (size == 0);
  }

  public int size(){
    return size;
  }

  public void clear(){
    size = 0;
    elements.clear();
    sorted = true;
  }

  public List<V> values(){
    List<V> values = new ArrayList<V>();
    for(int i = 0; i < size; i++){
      values.add(elements.get(i).getValue());
    }
    return values;
  }

  public void put(K key, V value){
//    int no = getNumber(key);
//    if(no != -1){
//      keys.set(no, key);
//      values.set(no, value);
//    }
    sorted = false;
    elements.add(new Element(key, value));
  }

  public void insert(K key, V value){
    if(!sorted)  sort();
    int i = 0;
    int j = size - 1;
    if(comparator != null){
      while(i != j){
        int m = (i + j) / 2;
        int cmp = comparator.compare(elements.get(m).getKey(), key);
        if(cmp < 0)  i = m;
        else if(cmp > 0)  j = m;
        else{
          elements.set(m, new Element(key, value));
        }
      }
      int cm = comparator.compare(elements.get(i).getKey(), key);
      if(cm < 0){
        elements.add(i+1, new Element(key, value));
      }else{
        elements.add(i, new Element(key, value));
      }
      size++;
    }else{
      while(i != j){
        int m = (i + j) / 2;
        int cmp = ((Comparable<? super K>)elements.get(m).getKey()).compareTo(key);
        if(cmp < 0)  i = m;
        else if(cmp > 0)  j = m;
        else{
          elements.set(m, new Element(key, value));
        }
      }
      int cm = ((Comparable<? super K>)elements.get(i).getKey()).compareTo(key);
      if(cm < 0){
          elements.add(i+1, new Element(key, value));
        }else{
          elements.add(i, new Element(key, value));
        }
      size++;
    }
  }

  public void sort(){
    sort(0, (size - 1));
  }

  private void sort(int min, int max){
    if(min == max)  return;
    int x = (min + max) / 2;
    sort(min, x);
    sort((x + 1), max);
    int i = min;
    int j = x + 1;
    List<Element> ele = new ArrayList<Element>();
    if(comparator != null){
      while(i <= x && j <= max){
        K ik = elements.get(i).getKey();
        K jk = elements.get(j).getKey();
        if(comparator.compare(ik, jk) > 0){
          ele.add(elements.get(j));
          j++;
        }else{
          ele.add(elements.get(i));
          i++;
        }
      }
      if(i <= x)  for(; i <= x; i++)  ele.add(elements.get(i));
      else if(j <= max)  for(; j <= max; j++)  ele.add(elements.get(j));
    }else{
      while(i <= x && j <= max){
        K ik = elements.get(i).getKey();
        K jk = elements.get(j).getKey();
        if(((Comparable<? super K>)ik).compareTo(jk) > 0){
          ele.add(elements.get(j));
          j++;
        }else{
          ele.add(elements.get(i));
          i++;
        }
      }
      if(i <= x)  for(; i <= x; i++)  ele.add(elements.get(i));
      else if(j <= max)  for(; j <= max; j++)  ele.add(elements.get(j));
    }
    assert(ele.size() == (max -min +1));
    for(int m = 0; m <= ele.size(); m++)  elements.set((min + m), ele.get(m));
  }
}
