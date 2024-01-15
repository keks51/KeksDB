package com.keks.kv_storage.kv_table_conf;

import com.fasterxml.jackson.databind.JsonNode;
import com.keks.kv_storage.conf.Params;

import java.util.Properties;


public class KvTableConf extends Params<KvTableConfParamsEnum> {

    public final int commitLogParallelism = super.getConfParam(KvTableConfParamsEnum.COMMIT_LOG_PARALLELISM);
    public final boolean enableCheckpoint = super.getConfParam(KvTableConfParamsEnum.ENABLE_PERIODIC_CHECKPOINT);
    public final int triggerCheckpointAfterSec = super.getConfParam(KvTableConfParamsEnum.TRIGGER_CHECKPOINT_AFTER_SEC);

    public KvTableConf(Properties properties) {
        super(KvTableConfParamsEnum.values(), properties);
    }

    public KvTableConf(JsonNode rootNode) {
        super(KvTableConfParamsEnum.values(), rootNode);
    }

    public KvTableConf(int commitLogParallelism) {
        super(KvTableConfParamsEnum.values(), new Properties() {{
            put(KvTableConfParamsEnum.COMMIT_LOG_PARALLELISM, commitLogParallelism);
            put(KvTableConfParamsEnum.ENABLE_PERIODIC_CHECKPOINT, false);
        }});
    }

    public KvTableConf(int commitLogParallelism,
                       boolean enableCheckpoint,
                       int triggerCheckpointAfterSec) {
        super(KvTableConfParamsEnum.values(), new Properties() {{
            put(KvTableConfParamsEnum.COMMIT_LOG_PARALLELISM, commitLogParallelism);
            put(KvTableConfParamsEnum.ENABLE_PERIODIC_CHECKPOINT, enableCheckpoint);
            put(KvTableConfParamsEnum.TRIGGER_CHECKPOINT_AFTER_SEC, triggerCheckpointAfterSec);
        }});
    }

}
