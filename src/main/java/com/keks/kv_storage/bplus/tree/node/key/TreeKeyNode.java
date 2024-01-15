package com.keks.kv_storage.bplus.tree.node.key;

import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.tree.node.KeysBinarySearch;
import com.keks.kv_storage.bplus.tree.node.TreeNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class TreeKeyNode extends TreeNode<KeyLocation> {

    public final KeyNodeKeys keysAndChildren = (KeyNodeKeys) super.keys;

//    private final ConcurrentHashMap<KeyLocation, String> keysCache = new ConcurrentHashMap<>();

    public TreeKeyNode(long pageId,
                       BtreeConf btreeConf) {
        super(pageId, btreeConf, new ConcurrentHashMap<>());
        super.setAsKeyNode();
    }

    public TreeKeyNode(ByteBuffer bb,
                       BtreeConf btreeConf) {
        super(bb, btreeConf, new ConcurrentHashMap<>());
    }

    @Override
    protected KeysBinarySearch<KeyLocation> buildKeys(ConcurrentHashMap<KeyLocation, String> keysCache) {
        return new KeyNodeKeys(
                super.getNextKeyBuf(),
                super.getKeysBuf(),
                btreeConf,
                keysCache);
    }

    public static AtomicInteger getCnt = new AtomicInteger();
    public static AtomicInteger missedCnt = new AtomicInteger();

    @Override
    public String getKey(KeyLocation keyLocation, IndexPageManager indexPageManager) throws IOException {
        String key = keysCache.get(keyLocation);
        if (key == null) {
            key = indexPageManager.getKey(keyLocation);
            keysCache.put(keyLocation, key);
            missedCnt.incrementAndGet();
        } else {
            getCnt.incrementAndGet();
        }
        return key;
    }


    public long getChildPageId(String key, IndexPageManager indexPageManager) throws IOException {
        int keyNum = getInsertPos(key, indexPageManager);
        if (keyNum < 0) {
            return keysAndChildren.getLeftChild(Math.abs(keyNum + 1));
        } else {
            return keysAndChildren.getRightChild(keyNum);
        }
    }


    public String toString1(IndexPageManager indexPageManager) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(keysAndChildren.getLeftChild(0));
        for (int i = 0; i < btreeConf.leafNodeKVArrSize; i++) {
            if (i < keysAndChildren.getNextKeyNum()) {
                KeyLocation indexLocation = keysAndChildren.getKeyLocation(i);
                String key;
                try {
                    key = indexPageManager.getKey(indexLocation);
                } catch (Throwable e) {
                    System.out.println("Cannot find key in location: " + indexLocation);
                    throw e;
                }

                sb.append("[" + key + "]");
                sb.append(keysAndChildren.getRightChild(i));
            } else {
                sb.append("[n]");
                sb.append(keysAndChildren.getRightChild(i));
            }
        }
        return "L:" + getLeftSibling() + " P:" + pageId + " Pr:" + getParent() + " " + sb.toString() + " R:" + getRightSibling() + "     ";
    }

}
