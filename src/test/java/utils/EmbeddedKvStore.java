package utils;

import com.keks.kv_storage.Main;
import com.keks.kv_storage.client.KVServerHttpClient;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;


public class EmbeddedKvStore {

    private final File kvStoreDir;
    private final int httpPort;
    private int httpServerMaxThreads;
    private final int thriftPort;

    private Process process;

    public EmbeddedKvStore(File kvStoreDir,
                           int httpPort,
                           int httpServerMaxThreads,
                           int thriftPort) {

        this.kvStoreDir = kvStoreDir;
        this.httpPort = httpPort;
        this.httpServerMaxThreads = httpServerMaxThreads;
        this.thriftPort = thriftPort;
    }

    public void start() throws IOException, URISyntaxException, InterruptedException {
        String classpath1 = System.getProperty("java.class.path");
        String[] entries = classpath1.split(File.pathSeparator);
        URL[] result = new URL[entries.length];
        for (int i = 0; i < entries.length; i++) {
            result[i] = Paths.get(entries[i]).toAbsolutePath().toUri().toURL();
        }
        String classpath = Arrays.stream(result).map(URL::getFile)
                .collect(Collectors.joining(File.pathSeparator));
        ProcessBuilder processBuilder = new ProcessBuilder(
                System.getProperty("java.home") + "/bin/java",
                "-classpath",
                classpath,
                Main.class.getName()
                // main class arguments go here
        )
                .inheritIO();
        Map<String, String> envVars = processBuilder.environment();
        envVars.put("KV_STORAGE_PATH", kvStoreDir.getAbsolutePath());
        envVars.put("KV_HTTP_SERVER_PORT", String.valueOf(httpPort));
        envVars.put("KV_HTTP_SERVER_MIN_THREADS", "1");
        envVars.put("KV_HTTP_SERVER_MAX_THREADS", String.valueOf(httpServerMaxThreads));
        envVars.put("KV_THRIFT_SERVER_PORT", String.valueOf(thriftPort));
        envVars.put("KV_THRIFT_SERVER_MIN_THREADS", "1");
        envVars.put("KV_THRIFT_SERVER_MAX_THREADS", "5");

        process = processBuilder.start();
        System.out.println("EmbeddedKvStore PID: " + process.pid());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            process.destroyForcibly();
            System.out.println("exit");
        }));



        KVServerHttpClient client = new KVServerHttpClient("localhost", httpPort);
        boolean serverIsUp = false;
        while (!serverIsUp) {
            try {
                Thread.sleep(1000);
                HashSet<String> stringHttpResponse = client.sendGetDBsRequest();
                System.out.println(stringHttpResponse);
                serverIsUp = true;
            } catch (ConnectException ignored) {

            }
        }
    }

    public void shutdown() {
        process.destroy();
    }

    public void shutdownForce() {
        process.destroyForcibly();
    }



}
