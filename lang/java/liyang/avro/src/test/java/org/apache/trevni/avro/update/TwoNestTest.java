package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;

public class TwoNestTest {
  public static void main(String[] args) throws IOException{
    Schema oSchema = new Schema.Parser().parse(new File("/home/ly/schemas/orders.avsc"));
    Schema lSchema = new Schema.Parser().parse(new File("/home/ly/schemas/lineitem.avsc"));
    Schema cSchema = new Schema.Parser().parse(new File("/home/ly/schemas/customer.avsc"));
    NestSchema cNS = new NestSchema(cSchema, new int[]{0});
    NestSchema oNS = new NestSchema(oSchema, new int[]{0}, new int[]{1});
    NestSchema lNS = new NestSchema(lSchema, new int[]{0,3}, new int[]{0});
    cNS.setPrFile(new File("/home/ly/customer.tbl"));
    oNS.setPrFile(new File("/home/ly/orders.tbl"));
    lNS.setPrFile(new File("/home/ly/lineitem.tbl"));
    cNS.setPath("/home/ly/test/customer/");
    oNS.setPath("/home/ly/test/orders/");
    lNS.setPath("/home/ly/test/lineitem/");
    cNS.setBloomFile(new File("/home/ly/test/cBloom"));
    oNS.setBloomFile(new File("/home/ly/test/oBloom"));
    lNS.setBloomFile(new File("/home/ly/test/lBloom"));
    NestedLoad load = new NestedLoad(new NestSchema[]{cNS, oNS, lNS}, "/home/ly/test/tmp/", "/home/ly/test/result/");
    load.load();
  }
}
