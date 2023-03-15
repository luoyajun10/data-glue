package com.data.integration.core.model;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.ProtobufIDLGenerator;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

import java.util.List;

public class RdsModelProtobuf {

    //返回的内容即为 Protobuf的IDL描述文件
    public static String protobufCode = ProtobufIDLGenerator.getIDL(RdsModelProtobuf.class);

    @Protobuf(fieldType = FieldType.STRING, order = 1, required=false, description="key: 库名.表名")
    private String key;

    @Protobuf(fieldType = FieldType.STRING, order = 2, required=false, description="主键列表，逗号分隔")
    private String primaryKeys;

    @Protobuf(fieldType = FieldType.STRING, order = 3, required=false, description="操作类型，INSERT,UPDATE,DELETE")
    private String opt;

    @Protobuf(fieldType = FieldType.OBJECT, order = 4, required=false, description="字段列表")
    private List<TbField> fieldList;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public String getOpt() {
        return opt;
    }

    public void setOpt(String opt) {
        this.opt = opt;
    }

    public List<TbField> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<TbField> fieldList) {
        this.fieldList = fieldList;
    }
}
