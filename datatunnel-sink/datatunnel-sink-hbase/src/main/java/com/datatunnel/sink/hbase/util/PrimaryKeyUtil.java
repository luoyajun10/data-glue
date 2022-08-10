package com.datatunnel.sink.hbase.util;

import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PrimaryKeyUtil {

    static final int INTEGER_BYTES_LENGTH = 4;
    static final int MD5_BYTES_LENGTH_SHORT = 4;
    public static final int TYPE_STRING = 0;
    public static final int TYPE_NUMBER = 1;
    public static final int TYPE_BINARY = 2;
    public static final int ROWKEY_TYPE_RANGE = 1;
    public static final int ROWKEY_TYPE_HASH = 0;

    public PrimaryKeyUtil() {
    }

    public static byte[] buildPrimaryKey(byte[] hashKey) {
        MessageDigest md5Digest;
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }

        md5Digest.update(hashKey);
        byte[] digest = md5Digest.digest();
        byte[] hashKeyLenByte = BytesUtil.toBytes(hashKey.length);
        byte[] primaryKey = new byte[4 + hashKeyLenByte.length + hashKey.length];
        BytesUtil.putBytes(primaryKey, 0, digest, 0, 4);
        BytesUtil.putBytes(primaryKey, 4, hashKeyLenByte, 0, hashKeyLenByte.length);
        BytesUtil.putBytes(primaryKey, 4 + hashKeyLenByte.length, hashKey, 0, hashKey.length);
        return primaryKey;
    }

    public static byte[] buildPrimaryKey(byte[] hashKey, byte[] rangeKey) {
        MessageDigest md5Digest;
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException var6) {
            var6.printStackTrace();
            return null;
        }

        md5Digest.update(hashKey);
        byte[] digest = md5Digest.digest();
        byte[] hashKeyLenByte = BytesUtil.toBytes(hashKey.length);
        byte[] primaryKey = new byte[4 + hashKeyLenByte.length + hashKey.length + rangeKey.length];
        BytesUtil.putBytes(primaryKey, 0, digest, 0, 4);
        BytesUtil.putBytes(primaryKey, 4, hashKeyLenByte, 0, hashKeyLenByte.length);
        BytesUtil.putBytes(primaryKey, 4 + hashKeyLenByte.length, hashKey, 0, hashKey.length);
        BytesUtil.putBytes(primaryKey, 4 + hashKeyLenByte.length + hashKey.length, rangeKey, 0, rangeKey.length);
        return primaryKey;
    }

    public static byte[] getHashKey(byte[] primaryKey) {
        byte[] hashKeyLenByte = BytesUtil.copy(primaryKey, 4, 4);
        int hashKeyLen = BytesUtil.toInt(hashKeyLenByte);
        return BytesUtil.copy(primaryKey, 8, hashKeyLen);
    }

    public static byte[] getRangeKey(byte[] primaryKey) {
        byte[] hashKeyLenByte = BytesUtil.copy(primaryKey, 4, 4);
        int hashKeyLen = BytesUtil.toInt(hashKeyLenByte);
        int hashKeyOffset = 8 + hashKeyLen;
        return BytesUtil.copy(primaryKey, 8 + hashKeyLen, primaryKey.length - hashKeyOffset);
    }

    public static byte[] buildPrimaryKey(Object hashKey, int hashKeyType) throws Exception {
        byte[] byteHashkey = null;
        if (hashKeyType == TYPE_STRING) {
            String strHashKey = hashKey.toString();
            byteHashkey = BytesUtil.toBytes(strHashKey);
        } else if (hashKeyType == TYPE_BINARY) {
            byteHashkey = Hex.decodeHex(hashKey.toString().toCharArray());
        } else if (hashKeyType == TYPE_NUMBER) {
            Long hashkey = Long.parseLong(hashKey.toString());
            byteHashkey = BytesUtil.toBytes(hashkey);
        }

        return buildPrimaryKey(byteHashkey);
    }

    public static byte[] buildPrimaryKey(Object hashKey, int hashKeyType, Object rangeKey, int rangeKeyType) throws Exception {
        byte[] byteHashkey = null;
        if (hashKeyType == 0) {
            String strHashKey = hashKey.toString();
            byteHashkey = BytesUtil.toBytes(strHashKey);
        } else if (hashKeyType == 2) {
            byteHashkey = Hex.decodeHex(hashKey.toString().toCharArray());
        } else if (hashKeyType == 1) {
            Long hashkey = Long.parseLong(hashKey.toString());
            byteHashkey = BytesUtil.toBytes(hashkey);
        }

        byte[] byteRangeKey = null;
        if (rangeKeyType == 0) {
            String strRangeKey = rangeKey.toString();
            byteRangeKey = BytesUtil.toBytes(strRangeKey);
        } else if (rangeKeyType == 2) {
            byteRangeKey = Hex.decodeHex(rangeKey.toString().toCharArray());
        } else if (rangeKeyType == 1) {
            Long rangekey = Long.parseLong(rangeKey.toString());
            byteRangeKey = BytesUtil.toBytes(rangekey);
        }

        return buildPrimaryKey(byteHashkey, byteRangeKey);
    }

    public static void main(String[] args) {
        byte[] primaryKey = buildPrimaryKey("abc".getBytes(), "word".getBytes());
        byte[] hashKey = getHashKey(primaryKey);
        System.out.println(BytesUtil.toString(hashKey));
        byte[] rangeKey = getRangeKey(primaryKey);
        System.out.println(BytesUtil.toString(rangeKey));
    }
}
