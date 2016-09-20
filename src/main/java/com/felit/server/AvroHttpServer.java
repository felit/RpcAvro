package com.felit.server;

import org.apache.avro.Protocol;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.generic.GenericResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 */
public class AvroHttpServer extends GenericResponder {
    private static Logger log = LoggerFactory.getLogger(AvroHttpServer.class);

    public AvroHttpServer(Protocol protocol) {
        super(protocol);
    }

    public Object respond(Protocol.Message message, Object request) throws Exception {
        GenericRecord req = (GenericRecord) request;
        GenericRecord reMessage = null;
        if (message.getName().equals("sayHello")) {
            Object name = req.get("name");
            //  do something...
            //取得返回值的类型
            reMessage = new GenericData.Record(super.getLocal().getType("User"));
            //直接构造回复
            reMessage.put("name", "Hello, " + name.toString());
            log.info(reMessage.toString());
        }
        return reMessage;
    }

    public static void main(String[] args) throws Exception {
        int port = 8088;
        try {
            Server server = new HttpServer(
                    new AvroHttpServer(Protocol.parse(new File("/data/source/self/RpcAvro/src/main/avro/mail.avro"))), port);
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
