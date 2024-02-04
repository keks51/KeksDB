package com.keks.kv_storage.lsm.conf;

import com.fasterxml.jackson.databind.JsonNode;
import com.keks.kv_storage.conf.Params;

import java.util.Objects;
import java.util.Properties;


public class LsmConf extends Params<LsmConfParamsEnum> {

    public final int maxSSTables = super.getConfParam(LsmConfParamsEnum.MAX_SSTABLES);
    public final boolean syncWithThreadFlush = super.getConfParam(LsmConfParamsEnum.SYNC_WITH_THREAD_FLUSH);
    public final boolean enableMergeIfMaxSSTables = super.getConfParam(LsmConfParamsEnum.ENABLE_MERGE_IF_MAX_SSTABLES);
    public final boolean enableBackgroundMerge = super.getConfParam(LsmConfParamsEnum.ENABLE_BACKGROUND_MERGE);
    public final int backgroundMergeInitDelay = super.getConfParam(LsmConfParamsEnum.BACKGROUND_MERGE_INIT_DELAY);
    public final int triggerBackgroundMergeAfterSec = super.getConfParam(LsmConfParamsEnum.TRIGGER_BACKGROUND_MERGE_AFTER_SEC);

    public final int sparseIndexSize = super.getConfParam(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS);
    public final int memCacheSize = super.getConfParam(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS);
    public final double bloomFilterFalsePositiveRate = super.getConfParam(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE);

    public LsmConf(Properties properties) {
        super(LsmConfParamsEnum.values(), properties);
    }

    public LsmConf(JsonNode rootNode) {
        super(LsmConfParamsEnum.values(), rootNode);
    }

    public LsmConf(int sparseIndexSize,
                   int memCacheSize,
                   double bloomFilterFalsePositiveRate) {
        super(LsmConfParamsEnum.values(), new Properties() {{
            put(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS, sparseIndexSize);
            put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, memCacheSize);
            put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, bloomFilterFalsePositiveRate);
        }});
    }

    public LsmConf(int sparseIndexSize,
                   int memCacheSize,
                   double bloomFilterFalsePositiveRate,
                   boolean syncWithThreadFlush) {
        super(LsmConfParamsEnum.values(), new Properties() {{
            put(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS, sparseIndexSize);
            put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, memCacheSize);
            put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, bloomFilterFalsePositiveRate);
            put(LsmConfParamsEnum.SYNC_WITH_THREAD_FLUSH, syncWithThreadFlush);
        }});
    }

    public LsmConf(int sparseIndexSize,
                   int memCacheSize,
                   double bloomFilterFalsePositiveRate,
                   boolean enableBackgroundMerge,
                   int backgroundMergeInitDelay,
                   int triggerBackgroundMergeAfterSec) {
        super(LsmConfParamsEnum.values(), new Properties() {{
            put(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS, sparseIndexSize);
            put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, memCacheSize);
            put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, bloomFilterFalsePositiveRate);
            put(LsmConfParamsEnum.ENABLE_BACKGROUND_MERGE, enableBackgroundMerge);
            put(LsmConfParamsEnum.BACKGROUND_MERGE_INIT_DELAY, backgroundMergeInitDelay);
            put(LsmConfParamsEnum.TRIGGER_BACKGROUND_MERGE_AFTER_SEC, triggerBackgroundMergeAfterSec);
        }});
    }

    public LsmConf(int sparseIndexSize,
                   int memCacheSize,
                   double bloomFilterFalsePositiveRate,
                   boolean enableMergeIfMaxSSTables,
                   int maxSSTables) {
        super(LsmConfParamsEnum.values(), new Properties() {{
            put(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS, sparseIndexSize);
            put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, memCacheSize);
            put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, bloomFilterFalsePositiveRate);
            put(LsmConfParamsEnum.ENABLE_MERGE_IF_MAX_SSTABLES, enableMergeIfMaxSSTables);
            put(LsmConfParamsEnum.MAX_SSTABLES, maxSSTables);
        }});
    }

    public LsmConf(int sparseIndexSize,
                   int memCacheSize,
                   double bloomFilterFalsePositiveRate,
                   boolean enableMergeIfMaxSSTables,
                   int maxSSTables,
                   boolean enableBackgroundMerge,
                   int backgroundMergeInitDelay,
                   int triggerBackgroundMergeAfterSec) {
        super(LsmConfParamsEnum.values(), new Properties() {{
            put(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS, sparseIndexSize);
            put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, memCacheSize);
            put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, bloomFilterFalsePositiveRate);
            put(LsmConfParamsEnum.ENABLE_MERGE_IF_MAX_SSTABLES, enableMergeIfMaxSSTables);
            put(LsmConfParamsEnum.MAX_SSTABLES, maxSSTables);
            put(LsmConfParamsEnum.ENABLE_BACKGROUND_MERGE, enableBackgroundMerge);
            put(LsmConfParamsEnum.BACKGROUND_MERGE_INIT_DELAY, backgroundMergeInitDelay);
            put(LsmConfParamsEnum.TRIGGER_BACKGROUND_MERGE_AFTER_SEC, triggerBackgroundMergeAfterSec);
        }});
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LsmConf lsmConf = (LsmConf) o;
        return sparseIndexSize == lsmConf.sparseIndexSize
                && memCacheSize == lsmConf.memCacheSize
                && Double.compare(lsmConf.bloomFilterFalsePositiveRate, bloomFilterFalsePositiveRate) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sparseIndexSize, memCacheSize, bloomFilterFalsePositiveRate);
    }

}
