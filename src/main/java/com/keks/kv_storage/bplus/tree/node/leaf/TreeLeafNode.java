package com.keks.kv_storage.bplus.tree.node.leaf;

import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.tree.node.KeysBinarySearch;
import com.keks.kv_storage.bplus.tree.node.TreeNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class TreeLeafNode extends TreeNode<LeafDataLocation> {

    public final LeafNodeKeys leafKeys = (LeafNodeKeys) super.keys;

    public TreeLeafNode(long pageId,
                        BtreeConf btreeConf) {
        super(pageId, btreeConf, new ConcurrentHashMap<>());
        super.setAsLeafNode();
    }

    public TreeLeafNode(ByteBuffer bb,
                        BtreeConf btreeConf) {
        super(bb, btreeConf, new ConcurrentHashMap<>());
    }

    @Override
    protected KeysBinarySearch<LeafDataLocation> buildKeys(ConcurrentHashMap<LeafDataLocation, String> keysCache) {
        return new LeafNodeKeys(super.getNextKeyBuf(), super.getKeysBuf(), btreeConf, keysCache);
    }

    public static AtomicInteger getCnt = new AtomicInteger();
    public static AtomicInteger missedCnt = new AtomicInteger();
    @Override
    public String getKey(LeafDataLocation keyLocation, IndexPageManager indexPageManager) throws IOException {
        String key = keysCache.get(keyLocation);
        if (key == null) {
            key = indexPageManager.getKeyToDataLocation(keyLocation).key;
            keysCache.put(keyLocation, key);
            missedCnt.incrementAndGet();
        } else {
            getCnt.incrementAndGet();
        }
        return key;
    }


    public String toString1(IndexPageManager indexPageManager) throws IOException {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < btreeConf.leafNodeKVArrSize; i++) {
            sb.append(i);
            if (i < leafKeys.getNextKeyNum()) {
                LeafDataLocation indexLocation = leafKeys.getKeyLocation(i);
                String key = indexPageManager.getKeyToDataLocation(indexLocation).key;
                sb.append(":" + key + " ");
            } else {
                sb.append(":n ");
            }
        }
        return "L:" + getLeftSibling() + " P:" + pageId + " Pr:" + getParent() + " " +  sb.toString() + " R:" + getRightSibling() + "     ";
    }

}
