package com.datatunnel.core.model;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

public class TbField {

    @Protobuf(fieldType = FieldType.STRING ,order = 1, required=false, description="字段名")
    public String fieldName;

    @Protobuf(fieldType = FieldType.STRING ,order = 2, required=false,  description="编码方式")
    public String encoding;

    @Protobuf(fieldType = FieldType.STRING ,order = 3, required=false, description="数据类型")
    public String type;

    @Protobuf(fieldType = FieldType.OBJECT ,order = 4, required=false, description="byte类型的值")
    public TbByteString tbByteString;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TbByteString getTbByteString() {
        return tbByteString;
    }

    public void setTbByteString(TbByteString tbByteString) {
        this.tbByteString = tbByteString;
    }
}
