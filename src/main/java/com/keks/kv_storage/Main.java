package com.keks.kv_storage;

import com.keks.kv_storage.server.http.KVHttpServer;

import com.keks.kv_storage.server.thrift.KVThriftServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.File;

import static com.keks.kv_storage.utils.EnvVar.getEnvVar;


public class Main {
//            System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Connection");


    static final Logger log = LogManager.getLogger(Main.class.getName());


    public static void main(String[] args) throws Exception {

        String kvStoragePath;
        int httpServerPort;
        int httpServerMinThreads;
        int httpServerMaxThreads;
        int thriftServerPort;
        int thriftServerMinThreads;
        int thriftServerMaxThreads;

        kvStoragePath = getEnvVar("KV_STORAGE_PATH");
        httpServerPort = Integer.parseInt(getEnvVar("KV_HTTP_SERVER_PORT"));
        httpServerMinThreads = Integer.parseInt(getEnvVar("KV_HTTP_SERVER_MIN_THREADS"));
        httpServerMaxThreads = Integer.parseInt(getEnvVar("KV_HTTP_SERVER_MAX_THREADS"));
        thriftServerPort = Integer.parseInt(getEnvVar("KV_THRIFT_SERVER_PORT"));
        thriftServerMinThreads = Integer.parseInt(getEnvVar("KV_THRIFT_SERVER_MIN_THREADS"));
        thriftServerMaxThreads = Integer.parseInt(getEnvVar("KV_THRIFT_SERVER_MAX_THREADS"));

        log.info("Storage path: '" + kvStoragePath + "'");
        log.info("httpServerPort: '" + httpServerPort + "'");
        log.info("httpServerMinThreads: '" + httpServerMinThreads + "'");
        log.info("httpServerMaxThreads: '" + httpServerMaxThreads + "'");
        log.info("thriftServerPort: '" + thriftServerPort + "'");
        log.info("thriftServerMinThreads: '" + thriftServerMinThreads + "'");
        log.info("thriftServerMaxThreads: '" + thriftServerMaxThreads + "'");

        KVThriftServer kvThriftServer;
        KVHttpServer kvHttpServer;


        KVStore kvStore = new KVStore(new File(kvStoragePath));
        kvThriftServer = new KVThriftServer(thriftServerPort, kvStore, thriftServerMinThreads, thriftServerMaxThreads);
        kvHttpServer = new KVHttpServer(httpServerPort, kvStore, httpServerMinThreads, httpServerMaxThreads);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown gracefully");
            kvHttpServer.stop();
            kvThriftServer.stop();
            kvStore.close();
            log.info("Successfully Shutdown");
        }));
        log.info("Starting server...");
        System.out.println("Starting server...");
        kvThriftServer.start();
        kvHttpServer.start();

    }
}
