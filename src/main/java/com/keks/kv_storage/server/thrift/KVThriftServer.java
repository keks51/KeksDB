package com.keks.kv_storage.server.thrift;


import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.server.KVServer;
import com.keks.kv_storage.server.ThreadUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.PrintWriter;
import java.io.StringWriter;


public class KVThriftServer extends KVServer {

    static final Logger log = LogManager.getLogger(KVThriftServer.class.getName());

    private final TThreadPoolServer tThreadPoolServer;
    private final int port;

    public KVThriftServer(int port, KVStore kvStore, int minThreads, int maxThreads) throws TTransportException {
        this.port = port;
        TServerSocket serverTransport = new TServerSocket(port);
        KVStoreProcessor kvStoreProcessor = new KVStoreProcessor(kvStore);

        TThreadPoolServer.Args args = new TThreadPoolServer
                .Args(serverTransport)
                .processor(kvStoreProcessor)
                .executorService(ThreadUtils.createLimitedExecutorService("KVThriftServer", minThreads, maxThreads, true));
        tThreadPoolServer = new TThreadPoolServer(args);
    }

    @Override
    public void start() {
        log.info("Starting Thrift server on port: " + this.port);
        Runnable runnable = tThreadPoolServer::serve;
        Thread thread = new Thread(runnable);
        thread.start();
        log.info("Thrift server is running on port: " + this.port);
    }

    @Override
    public void stop() {
        tThreadPoolServer.stop();
    }

    static public class KVStoreProcessor implements TProcessor {
        static final Logger log = LogManager.getLogger(KVThriftServer.class.getName());

        private final ThriftServerCommandHandler actionHandler;

        public KVStoreProcessor(KVStore kvStore) {
            this.actionHandler = new ThriftServerCommandHandler(kvStore);
        }

        @Override
        public void process(TProtocol in, TProtocol out) throws TException {
            try {
                in.readByte(); // skip next_message byte
                try {
                    actionHandler.executeCommand(in, out);
                } catch (Throwable e) {
                    System.out.println("Error occurred");
                    handleException(out, e);
                } finally {
                    out.getTransport().flush();
                }

            } catch (TTransportException e) {
                if (!e.getMessage().contains("Socket is closed by peer.")) e.printStackTrace();
                log.info("Client was disconnected");
                throw e; // indicates outer thread to stop processing
            }
        }

        private void handleException(TProtocol out, Throwable t) throws TException {
            StringWriter errors = new StringWriter();
            t.printStackTrace(new PrintWriter(errors));
            String res = "'\n" + errors;
            out.writeByte((byte) 1);
            out.writeString(t.getClass().getName());
            out.writeString(res);
        }

    }

}
