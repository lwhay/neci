package org.apache.trevni.avro.update;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

public class EncodeSchema {
  private Schema schema;
  private Schema encode;

  public EncodeSchema(Schema schema){
    this.schema = schema;
    encode();
  }

  public void encode(){
    encode = encode('a', schema);
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

  public Schema getEncode(){
    if(encode != null)  return encode;
    else   return schema;
  }
}
