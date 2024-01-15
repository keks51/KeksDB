package com.keks.kv_storage.conf;

public class ConfigParams {

    public static final String DATABASE_NAME = "DATABASE_NAME";
    public static final String TABLE_NAME = "TABLE_NAME";
    public static final String ENGINE_NAME = "ENGINE_NAME";
    public static final String KEY_NAME = "KEY_NAME";
    public static final String VALUE_DATA = "VALUE_DATA";
    public static final String BPLUS_TREE_ORDER = "BPLUS_TREE_ORDER";
    public static final String BPLUS_TREE_PAGE_BUFFER_SIZE_BYTES = "BPLUS_TREE_PAGE_BUFFER_SIZE_BYTES";
    public static final String BPLUS_FREE_SPACE_CHECKER_CACHE_MAX = "BPLUS_FREE_SPACE_CHECKER_CACHE_MAX";
    public static final String BPLUS_FREE_SPACE_CHECKER_CACHE_INIT = "BPLUS_FREE_SPACE_CHECKER_CACHE_INIT";
    public static final String LSM_MAX_SSTABLES = "LSM_MAX_SSTABLES";
    public static final String LSM_ENABLE_MERGE_IF_MAX_SSTABLES = "ENABLE_MERGE_IF_MAX_SSTABLES";
    public static final String LSM_ENABLE_BACKGROUND_MERGE = "ENABLE_BACKGROUND_MERGER";
    public static final String LSM_BACKGROUND_MERGE_INIT_DELAY = "BACKGROUND_MERGE_INIT_DELAY";
    public static final String LSM_TRIGGER_BACKGROUND_MERGE_AFTER_SEC = "TRIGGER_SSTABLES_MERGE_AFTER_SEC";
    public static final String LSM_SPARSE_INDEX_SIZE = "LSM_SPARSE_INDEX_SIZE";
    public static final String LSM_MEM_CACHE_SIZE = "LSM_MEM_CACHE_SIZE";
    public static final String LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE = "LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE";
    public static final String KV_TABLE_COMMIT_LOG_PARALLELISM = "KV_TABLE_COMMIT_LOG_PARALLELISM";

    public static final String KV_TABLE_ENABLE_PERIODIC_CHECKPOINT = "KV_TABLE_ENABLE_PERIODIC_CHECKPOINT";
    public static final String KV_TABLE_TRIGGER_CHECKPOINT_AFTER_SEC = "KV_TABLE_TRIGGER_CHECKPOINT_AFTER_SEC";



}
