package com.keks.kv_storage.bplus.conf;

import com.fasterxml.jackson.databind.JsonNode;
import com.keks.kv_storage.conf.Params;

import java.util.Properties;


public class BPlusConf extends Params<BPlusConfParamsEnum> {

    public final BtreeConf btreeConf = new BtreeConf(super.getConfParam(BPlusConfParamsEnum.BTREE_ORDER));

    public final int freeSpaceCheckerMaxCache = super.getConfParam(BPlusConfParamsEnum.FREE_SPACE_CHECKER_CACHE_MAX);
    public final int freeSpaceCheckerInitCache = super.getConfParam(BPlusConfParamsEnum.FREE_SPACE_CHECKER_CACHE_INIT);
    public final long pageBufferSizeBytes = super.getConfParam(BPlusConfParamsEnum.BTREE_PAGE_BUFFER_SIZE_BYTES);

    public BPlusConf(Properties properties) {
        super(BPlusConfParamsEnum.values(), properties);
    }

    public BPlusConf(JsonNode rootNode) {
        super(BPlusConfParamsEnum.values(), rootNode);
    }

    public BPlusConf(int treeOrder,
                     int freeSpaceCheckerInitCache,
                     int freeSpaceCheckerMaxCache,
                     long pageBufferSizeBytes) {
        super(BPlusConfParamsEnum.values(), new Properties() {{
            put(BPlusConfParamsEnum.BTREE_ORDER, treeOrder);
            put(BPlusConfParamsEnum.BTREE_PAGE_BUFFER_SIZE_BYTES, pageBufferSizeBytes);
            put(BPlusConfParamsEnum.FREE_SPACE_CHECKER_CACHE_INIT, freeSpaceCheckerInitCache);
            put(BPlusConfParamsEnum.FREE_SPACE_CHECKER_CACHE_MAX, freeSpaceCheckerMaxCache);
        }});
    }



}
