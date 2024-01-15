package com.keks.kv_storage;

import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.ss_table.SSTable;
import com.keks.kv_storage.lsm.ss_table.SSTableMetadata;

import java.util.LinkedList;
import java.util.List;


// TODO add json annotations to remove getters and setters
public class KVTableStatistics {

    private final int totalNumberOfRecordsWithDeleted;
    private final int totalNumberOfDeletedRecords;
    private final int totalNumberOfRecords;
    private final int totalNumberOfSSTables;
    private final int numberOfRecordsInMemoryWithDeleted;
    private final int numberOfDeletedRecordsInMemory;
    private final double tableSizeOnDiskMBytes;
    private final double totalBloomFilterMemoryConsumptionKBytes;
    private final double bloomFilterFalsePositiveRate;
    private final String kvTableName;
    private final List<SSTableInfo> ssTableInfoList;

    public List<SSTableInfo> getSsTableInfoList() {
        return ssTableInfoList;
    }

    public int getTotalNumberOfSSTables() {
        return totalNumberOfSSTables;
    }

    public int getTotalNumberOfRecordsWithDeleted() {
        return totalNumberOfRecordsWithDeleted;
    }

    public int getTotalNumberOfDeletedRecords() {
        return totalNumberOfDeletedRecords;
    }

    public int getTotalNumberOfRecords() {
        return totalNumberOfRecords;
    }

    public int getNumberOfRecordsInMemoryWithDeleted() {
        return numberOfRecordsInMemoryWithDeleted;
    }

    public int getNumberOfDeletedRecordsInMemory() {
        return numberOfDeletedRecordsInMemory;
    }

    public String getKvTableName() {
        return kvTableName;
    }

    public double getTotalBloomFilterMemoryConsumptionKBytes() {
        return totalBloomFilterMemoryConsumptionKBytes;
    }

    public double getBloomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    public double getTableSizeOnDiskMBytes() {
        return tableSizeOnDiskMBytes;
    }

    static class SSTableInfo {
        public final String dirPath;
        public final int sparseIndexSize;
        public final int numberOfRecordsWithDeleted;
        public final int deletedRecords;
        public final double bloomFilterMemoryConsumptionKBytes;

        public SSTableInfo(String dirPath,
                           SSTableMetadata ssTableMetadata,
                           double bloomFilterMemoryConsumptionKBytes) {
            this.dirPath = dirPath;
            this.sparseIndexSize = ssTableMetadata.sparseIndexSize;
            this.numberOfRecordsWithDeleted = ssTableMetadata.numberOfRecordsWithDeleted;
            this.deletedRecords = ssTableMetadata.deletedRecords;
            this.bloomFilterMemoryConsumptionKBytes = bloomFilterMemoryConsumptionKBytes;
        }
    }

    public KVTableStatistics(String name,
                             int recordsInMemoryWithDeleted,
                             int numberOfDeletedRecordsInMemory,
                             double directorySizeInMBytes,
                             List<SSTable> ssTables,
                             LsmConf lsmConf) {
        int totalNumberOfRecordsCount = 0;
        int totalNumberOfDeletedRecordsCount = 0;
        double totalBloomFilterMemoryConsumptionKBytes = 0;
        List<SSTableInfo> infoList = new LinkedList<>();
        for (SSTable ssTable: ssTables) {
            totalNumberOfRecordsCount = totalNumberOfRecordsCount + ssTable.ssTableMetadata.numberOfRecordsWithDeleted;
            totalNumberOfDeletedRecordsCount = totalNumberOfDeletedRecordsCount + ssTable.ssTableMetadata.deletedRecords;
            totalBloomFilterMemoryConsumptionKBytes = totalBloomFilterMemoryConsumptionKBytes + ssTable.bloomFilter.memoryConsumptionKBytes;
            infoList.add(new SSTableInfo(ssTable.ssTableDirPath.getAbsolutePath(), ssTable.ssTableMetadata, ssTable.bloomFilter.memoryConsumptionKBytes));
        }
        this.ssTableInfoList = infoList;
        this.totalNumberOfSSTables = ssTables.size();
        this.numberOfRecordsInMemoryWithDeleted = recordsInMemoryWithDeleted;
        this.numberOfDeletedRecordsInMemory = numberOfDeletedRecordsInMemory;
        this.tableSizeOnDiskMBytes = directorySizeInMBytes;
        this.kvTableName = name;
        this.totalNumberOfRecordsWithDeleted = totalNumberOfRecordsCount + recordsInMemoryWithDeleted;
        this.totalNumberOfDeletedRecords = totalNumberOfDeletedRecordsCount + numberOfDeletedRecordsInMemory;
        this.totalNumberOfRecords = totalNumberOfRecordsWithDeleted - totalNumberOfDeletedRecords;
        this.totalBloomFilterMemoryConsumptionKBytes = totalBloomFilterMemoryConsumptionKBytes;
        this.bloomFilterFalsePositiveRate = lsmConf.bloomFilterFalsePositiveRate;
    }

}


