package com.keks.kv_storage.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.server.KVServer;
import com.keks.kv_storage.server.ThreadUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URI;


public class KVHttpServer extends KVServer {

    static final Logger log = LogManager.getLogger(KVHttpServer.class.getName());

    private final int port;
    private final HttpServer server;
    private final int backLog = 50_000;

    public KVHttpServer(int port, KVStore kvStore, int minThreads, int maxThreads) throws IOException {

        this.port = port;
        this.server = HttpServer.create();
        server.bind(new InetSocketAddress(port), backLog);
        server.createContext("/api1", new MyHttpHandler(kvStore));
        server.setExecutor(ThreadUtils.createLimitedExecutorService("KVHttpServer", minThreads, maxThreads, true));
    }

    @Override
    public void start() {
        try {
            log.info("Starting HTTP server on port: '" + port + "' with backlog: '" + backLog);
            server.start();
            log.info("HTTP server is running on port: '" + port + "' with backlog: '" + backLog);
        } catch (Throwable t) {

        }

    }

    @Override
    public void stop() {
        this.server.stop(1);
    }

    static class MyHttpHandler implements HttpHandler {

        private final HttpCommandHandler commandHandler;

        public MyHttpHandler(KVStore kvStore) {
            this.commandHandler = new HttpCommandHandler(kvStore);
        }

        @Override
        public void handle(HttpExchange httpExchange) {
            URI requestURI = httpExchange.getRequestURI();
            String actionStr = requestURI.getPath().split("/")[2]; // first is /api1
            try {
                commandHandler.executeCommand(requestURI, httpExchange);
            } catch (Throwable e) {
                e.printStackTrace();
                handleException(actionStr, httpExchange, e); // TODO 2 types of exceptions should be 1) before send headers and after
            }
        }

        public void handleException(String actionStr, HttpExchange httpExchange, Throwable t) {
            StringWriter errors = new StringWriter();
            t.printStackTrace(new PrintWriter(errors));
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put("message", "Cannot execute: '" + actionStr + "'");
            objectNode.put("errorClass", t.getClass().getName());
            objectNode.put("error", errors.toString());
            String json = objectNode.toPrettyString();
            try (OutputStream outputStream = httpExchange.getResponseBody()) {

                httpExchange.sendResponseHeaders(500, json.length());
                outputStream.write(json.getBytes());
                outputStream.flush();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

    }


}


