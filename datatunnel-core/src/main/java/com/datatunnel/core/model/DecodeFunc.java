package com.datatunnel.core.model;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;

import java.io.IOException;

public class DecodeFunc {

    public static RdsModelProtobuf decode(byte[] bytes) throws IOException {
        Codec<RdsModelProtobuf> dCodec = ProtobufProxy.create(RdsModelProtobuf.class);
        return dCodec.decode(bytes);
    }
}
