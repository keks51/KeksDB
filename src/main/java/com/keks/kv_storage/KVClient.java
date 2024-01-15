package com.keks.kv_storage;

import com.keks.kv_storage.server.http.TestHttpKVStorageWritePerformance;
import com.keks.kv_storage.server.thrift.TestThriftKVStorageWritePerformance;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.keks.kv_storage.utils.EnvVar.getEnvVar;


public class KVClient {

    public static void main(String[] args) throws URISyntaxException, IOException, ExecutionException, InterruptedException, TimeoutException, TException {
        String clientType = getEnvVar("CLIENT_TYPE");
        System.out.println("ClientType: " + clientType);
        String hostName = getEnvVar("KV_SERVER_HOST");
        System.out.println("HostName: " + hostName);

        int httpServerPort = Integer.parseInt(getEnvVar("KV_HTTP_SERVER_PORT"));
        System.out.println("HTTPServerPort: " + httpServerPort);

        int thriftServerPort = Integer.parseInt(getEnvVar("KV_THRIFT_SERVER_PORT"));
        System.out.println("ThriftServerPort: " + thriftServerPort);

        int recordsNumber = Integer.parseInt(getEnvVar("RECORDS_NUMBER"));
        System.out.println("RecordsNumber: " + recordsNumber);

        int numberOfThreads = Integer.parseInt(getEnvVar("NUMBER_OF_THREADS"));
        System.out.println("NumberOfThreads: " + numberOfThreads);

        int sparseIndexSize = Integer.parseInt(getEnvVar("SPARSE_INDEX_SIZE"));
        System.out.println("SparseIndexSize: " + sparseIndexSize);

        int memCacheSize = Integer.parseInt(getEnvVar("MEM_CACHE_SIZE"));
        System.out.println("memCacheSize: " + memCacheSize);

        double bloomFilterFalsePositiveRate = Double.parseDouble(getEnvVar("BLOOM_FILTER_FALSE_POSITIVE_RATE"));
        System.out.println("BloomFilterFalsePositiveRate: " + bloomFilterFalsePositiveRate);

        String httpTableName = getEnvVar("HTTP_TABLE_NAME");
        System.out.println("HTTPTableName: " + httpTableName);

        String thriftTableName = getEnvVar("THRIFT_TABLE_NAME");
        System.out.println("ThriftTableName: " + thriftTableName);

        if(clientType.equals("HTTP")) {
            TestHttpKVStorageWritePerformance.run(hostName, httpServerPort, recordsNumber, numberOfThreads, sparseIndexSize, memCacheSize, bloomFilterFalsePositiveRate, httpTableName);
        } else if (clientType.equals("THRIFT")) {
//            TestThriftKVStorageWritePerformance.run(hostName, thriftServerPort, recordsNumber, numberOfThreads, sparseIndexSize, inMemoryRecords, bloomFilterFalsePositiveRate, thriftTableName);
        }

    }
}
