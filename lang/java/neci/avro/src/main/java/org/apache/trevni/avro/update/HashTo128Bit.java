package org.apache.trevni.avro.update;

import org.apache.avro.generic.GenericData.Record;

public class HashTo128Bit {

  public static long rotl64(long v, int n){
    return ((v << n) | (v >>> (64 - n)));
  }

  public static long fmix(long k){
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }

  public static void hashto128(Record record, long seed, long[] hashes){
    hashto128(new KeyToBytes(record), seed, hashes);
  }

  public static void hashto128(Record record, int[] keyFields, long seed, long[] hashes){
    hashto128(new KeyToBytes(record, keyFields), seed, hashes);
  }

  public static void hashto128(KeyToBytes by, long seed, long[] hashes){
    //KeyToBytes by = new KeyToBytes(record);
    int length = by.getLength();
    final int nblocks = length >> 4;

    long h1 = seed;
    long h2 = seed;

    long c1 = 0x87c37b91114253d5L;
    long c2 = 0x4cf5ad432745937fL;

    byte[][] bytes = by.getBytes();
    int currentFieldIndex = 0;
    int bytePos = 0;
    for(int i = 0; i < nblocks; ++i){
      long k1 = 0L;
      for(int j = 0; j < 8; ++j){
        k1 += (((long)bytes[currentFieldIndex][bytePos]) & 0xff) << (j << 3);
        ++bytePos;
        if(bytePos == by.getFieldLength(currentFieldIndex)){
          ++currentFieldIndex;
          bytePos = 0;
        }
      }
      long k2 = 0L;
      for(int j = 0; j < 8; ++j){
        k2 += (((long)bytes[currentFieldIndex][bytePos]) & 0xff) << (j << 3);
        ++bytePos;
        if(bytePos == by.getFieldLength(currentFieldIndex)){
          ++currentFieldIndex;
          bytePos = 0;
        }
      }

      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;

      h1 = rotl64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;

      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

      h2 = rotl64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    long k1 = 0L;
    long k2 = 0L;

    currentFieldIndex = bytes.length - 1;
    bytePos = by.getFieldLength(currentFieldIndex) - 1;
    switch(length & 15){
    case 15:
      k2 ^= ((long)bytes[currentFieldIndex][bytePos]) << 48;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 14:
      k2 ^= ((long)bytes[currentFieldIndex][bytePos]) << 40;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 13:
      k2 ^= ((long)bytes[currentFieldIndex][bytePos]) << 32;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 12:
      k2 ^= ((long)bytes[currentFieldIndex][bytePos]) << 24;
        --bytePos;
        if(bytePos == -1){
          --currentFieldIndex;
          bytePos = by.getFieldLength(currentFieldIndex) - 1;
        }
    case 11:
      k2 ^= ((long)bytes[currentFieldIndex][bytePos]) << 16;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 10:
      k2 ^= ((long)bytes[currentFieldIndex][bytePos]) << 8;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 9:
      k2 ^= (long)bytes[currentFieldIndex][bytePos];
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
      k2 *= c2;
      k2 = rotl64(k2, 33);
      k2 *= c1;
      h2 ^= k2;

    case 8:
      k1 ^= ((long)bytes[currentFieldIndex][bytePos]) << 56;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 7:
      k1 ^= ((long)bytes[currentFieldIndex][bytePos]) << 48;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 6:
      k1 ^= ((long)bytes[currentFieldIndex][bytePos]) << 40;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 5:
      k1 ^= ((long)bytes[currentFieldIndex][bytePos]) << 32;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 4:
      k1 ^= ((long)bytes[currentFieldIndex][bytePos]) << 24;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
        }
    case 3:
      k1 ^= ((long)bytes[currentFieldIndex][bytePos]) << 16;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 2:
      k1 ^= ((long)bytes[currentFieldIndex][bytePos]) << 8;
      --bytePos;
      if(bytePos == -1){
        --currentFieldIndex;
        bytePos = by.getFieldLength(currentFieldIndex) - 1;
      }
    case 1:
      k1 ^= (long)bytes[currentFieldIndex][bytePos];
      --bytePos;
      k1 *= c1;
      k1 = rotl64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
    }

    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

    h1 += h2;
    h2 += h1;

    hashes[0] = h1;
    hashes[1] = h2;
  }
}
