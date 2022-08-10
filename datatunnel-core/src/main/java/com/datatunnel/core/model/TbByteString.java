package com.datatunnel.core.model;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;

public class TbByteString {

    @Protobuf(fieldType = FieldType.INT32 ,order = 1,required=false, description="byte[]类型的值长度")
    private int len;
    @Protobuf(fieldType = FieldType.BYTES ,order = 2, required=false, description="byte[]的值")
    private byte[] bytes;

    public void setLen(int len) {
        this.len = len;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public TbByteString(){}

    public TbByteString(byte[] bytes, int len) {
        if (len == 0) {
            this.bytes = new byte[1];
            this.bytes[0] = 0;
            this.len = 0;
        } else {
            this.bytes = bytes;
            this.len = len;
        }
    }

    public TbByteString(byte[] bytes, int offset, int len) {
        if (len == 0) {
            this.bytes = new byte[1];
            this.bytes[0] = 0;
            this.len = 0;
        } else {
            this.bytes = new byte[len];
            this.len = len;
            System.arraycopy(bytes, offset, this.bytes, 0, len);
        }
    }
    /**
     * 根据编码类型将数据转化为String
     * @param encoding
     * @return
     * @throws UnsupportedEncodingException
     */
    public String toString(String encoding) throws UnsupportedEncodingException {
        if (this.len == 0) {
            return StringUtils.EMPTY;
        }
        if (encoding.equalsIgnoreCase("binary")) {
            throw new UnsupportedEncodingException(
                    "field encoding: binary, use getBytes() instead of toString()");
        }

        String realEncoding = encoding;
        if ((encoding.isEmpty()) || (encoding.equalsIgnoreCase("null")))
            realEncoding = "ASCII";
        else if (encoding.equalsIgnoreCase("utf8mb4"))
            realEncoding = "utf8";
        else if (encoding.equalsIgnoreCase("latin1"))
            realEncoding = "cp1252";
        else if (encoding.equalsIgnoreCase("latin2")) {
            realEncoding = "iso-8859-2";
        }
        return new String(this.bytes, realEncoding);
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    public int getLen() {
        return this.len;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        TbByteString byteString = (TbByteString) o;
        if (byteString.len != this.len) {
            return false;
        }
        byte[] otherBytes = byteString.getBytes();
        for (int index = 0; index < this.len; ++index) {
            if (otherBytes[index] != this.bytes[index]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        if (this.len == 0) {
            return StringUtils.EMPTY;
        }
        int len = this.len;
        byte[] byteArray = this.bytes;
        char[] charArray = new char[len];
        for (int i = 0; i < len; ++i) {
            charArray[i] = (char) byteArray[i];
        }

        return String.valueOf(charArray);
    }
}
