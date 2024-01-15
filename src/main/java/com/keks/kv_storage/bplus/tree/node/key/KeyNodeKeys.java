package com.keks.kv_storage.bplus.tree.node.key;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.tree.node.KeysBinarySearch;
import com.keks.kv_storage.bplus.utils.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;


public class KeyNodeKeys extends KeysBinarySearch<KeyLocation> {

    private final int bbStartPosition;
    private final ByteBuffer nextKeyBB;
    private final ConcurrentHashMap<KeyLocation, String> keysCache;

    private volatile int nextKeyPos;

    private final ByteBuffer keysAndChildrenBB;
    private final BtreeConf btreeConf;

    public KeyNodeKeys(ByteBuffer nextKeyBB,
                       ByteBuffer keysAndChildrenBB,
                       BtreeConf btreeConf) {
        this(nextKeyBB, keysAndChildrenBB, btreeConf, new ConcurrentHashMap<>());

    }
    public KeyNodeKeys(ByteBuffer nextKeyBB,
                       ByteBuffer keysAndChildrenBB,
                       BtreeConf btreeConf,
                       ConcurrentHashMap<KeyLocation, String> keysCache) {
        this.nextKeyBB = nextKeyBB;
        this.keysCache = keysCache;
        nextKeyBB.reset();
        this.nextKeyPos = nextKeyBB.getInt();
        keysAndChildrenBB.reset();
        this.bbStartPosition = keysAndChildrenBB.position();
        this.keysAndChildrenBB = keysAndChildrenBB;
        this.btreeConf = btreeConf;
    }

    // keys should be one less than children for test purposes
    public void bulkInsertInEmptyNode(KeyLocation[] keys, long[] children) {
        int startPos = 0;
        seekToLeftChildOfKey(startPos);
        keysAndChildrenBB.reset();
        for (int i = startPos; i < keys.length; i++) {
            keysAndChildrenBB.putLong(children[i]);
            KeyLocation indexedKeyLocation = keys[i];
            keysAndChildrenBB.putLong(indexedKeyLocation.pageId);
            keysAndChildrenBB.putShort(indexedKeyLocation.slotId);
        }
        long lastChild = children[children.length - 1];
        keysAndChildrenBB.putLong(lastChild);
        keysAndChildrenBB.reset();
        setNextKeyNum(keys.length);
    }

//    public KeyLocation getKeyLocationOld(int keyNum) {
//        if (keyNum >= getNextKeyNum()) throw new IllegalArgumentException("NodeKey should be lower then: " + btreeConf.leafNodeKVArrSize);
//        seekToKey(keyNum);
//        return new KeyLocation(keysAndChildrenBB.getLong(), keysAndChildrenBB.getShort());
//    }

    public KeyLocation getKeyLocation(int keyNum) {
        if (keyNum >= getNextKeyNum()) throw new IllegalArgumentException("NodeKey should be lower then: " + btreeConf.leafNodeKVArrSize);
        int seekPos = TypeSize.LONG + keyNum * (TypeSize.LONG + KeyLocation.SIZE);
        return new KeyLocation(keysAndChildrenBB.getLong(bbStartPosition + seekPos), keysAndChildrenBB.getShort(bbStartPosition + seekPos + TypeSize.LONG));
    }

    public void moveRightHalfToNewRightNode(KeyNodeKeys rightNode) {
        int firstKeyNum = btreeConf.keysArrMidPointPos + 1;
        seekToRightChildOfKey(firstKeyNum);
        rightNode.seekToLeftChildOfKey(0);
        rightNode.setChild(getLeftChild(firstKeyNum));
        int cnt = 0;
        for (; firstKeyNum < btreeConf.leafNodeKVArrSize; ++firstKeyNum) {
            KeyLocation keyLocation = getKeyLocation(firstKeyNum);
            keysCache.remove(keyLocation);
            rightNode.setKey(keyLocation);
            rightNode.setChild(getRightChild(firstKeyNum));
            cnt++;
        }
        rightNode.setNextKeyNum(cnt);
        setNextKeyNum(btreeConf.keysArrMidPointPos);
    }

    public KeyLocation getMidIndex() {
        seekToKey(btreeConf.keysArrMidPointPos);
        return new KeyLocation(keysAndChildrenBB.getLong(), keysAndChildrenBB.getShort());
    }

    public boolean isFull() {
        return getNextKeyNum() == btreeConf.leafNodeKVArrSize;
    }

    public boolean isAlmostFull() {
        return getNextKeyNum() == btreeConf.leafNodeKVArrSize - 1;
    }

    public boolean isEmpty() {
        return getNextKeyNum() == 0;
    }

    public KeyLocationAndRightChild removeLastKeyAndRightChild() {
        try {
            int keyNum = getNextKeyNum() - 1;
            KeyLocation keyLocation = getKeyLocation(keyNum);
            keysCache.remove(keyLocation);
            return new KeyLocationAndRightChild(keyLocation, getRightChild(keyNum));
        } finally {
            decNextKeyNum();
        }
    }

    public KeyLocationAndLeftChild removeFirstKeyAndLeftChild() {
        return removeKeyAndLeftChild(0);
    }

    // TODO if getNextKeyNum == 0 after deleting then remove mostLeftChild of 0 keyNum or maybe not since no key available
    public KeyLocationAndRightChild removeKeyAndRightChild(int keyNum) {
        if (keyNum > getNextKeyNum()) throw new IllegalArgumentException("NodeKey");
        KeyLocation removedKey = getKeyLocation(keyNum);
        keysCache.remove(removedKey);
        long removedRightChild = getRightChild(keyNum);
        int srcPos = (keyNum + 1) * (TypeSize.LONG + KeyLocation.SIZE) + TypeSize.LONG;
        ByteBufferUtils.shiftLeft(keysAndChildrenBB, srcPos, (KeyLocation.SIZE + TypeSize.LONG));
        decNextKeyNum();
        return new KeyLocationAndRightChild(removedKey, removedRightChild);
    }

    public KeyLocationAndLeftChild removeKeyAndLeftChild(int keyNum) {
        if (keyNum > getNextKeyNum()) throw new IllegalArgumentException("NodeKey");
        KeyLocation removedKey = getKeyLocation(keyNum);
        keysCache.remove(removedKey);
        long removedLeftChild = getLeftChild(keyNum);
        int srcPos = (keyNum + 1) * (TypeSize.LONG + KeyLocation.SIZE);
        ByteBufferUtils.shiftLeft(keysAndChildrenBB, srcPos, (TypeSize.LONG + KeyLocation.SIZE));
        decNextKeyNum();
        return new KeyLocationAndLeftChild(removedKey, removedLeftChild);
    }

    public void joinWithRightNode(KeyNodeKeys rightNode) {
        seekToLeftChildOfKey(getNextKeyNum());
        setChild(rightNode.getLeftChild(0));
        int cnt = 0;
        for (int i = 0; i < rightNode.getNextKeyNum(); i++) {
            setKey(rightNode.getKeyLocation(i));
            setChild(rightNode.getRightChild(i));
            cnt++;
        }
        rightNode.keysCache.clear();
        rightNode.setNextKeyNum(0);
        setNextKeyNum(getNextKeyNum() + cnt);
    }

    public void joinWithLeftNode(KeyNodeKeys leftNode) {
        int shiftLen = (leftNode.getNextKeyNum()) * (KeyLocation.SIZE + TypeSize.LONG);
        ByteBufferUtils.shiftRight(keysAndChildrenBB, TypeSize.LONG, shiftLen);
        seekToLeftChildOfKey(0);
        setChild(leftNode.getLeftChild(0));
        int cnt = 0;
        for (int i = 0; i < leftNode.getNextKeyNum(); i++) {
            setKey(leftNode.getKeyLocation(i));
            setChild(leftNode.getRightChild(i));
            cnt++;
        }
        leftNode.keysCache.clear();
        leftNode.setNextKeyNum(0);
        setNextKeyNum(getNextKeyNum() + cnt);
    }

    public void insertKeyAndChildrenWhenEmpty(KeyLocation indexedKeyLocation, long leftChildPageId, long rightChildPageId) {
        seekToLeftChildOfKey(0);
        setChild(leftChildPageId);
        setKey(indexedKeyLocation);
        setChild(rightChildPageId);
        incNextKeyNum();
    }

    public void insertKeyAndRightChild(int keyNum, KeyLocation indexedKeyLocation, long rightChildPageId) {
        if (keyNum > getNextKeyNum()) throw new IllegalArgumentException("NodeKey");
        int srcPos = TypeSize.LONG + keyNum * (TypeSize.LONG + KeyLocation.SIZE);
        ByteBufferUtils.shiftRight(keysAndChildrenBB, srcPos, TypeSize.LONG + KeyLocation.SIZE);
        seekToKey(keyNum);
        setKey(indexedKeyLocation);
        setChild(rightChildPageId);
        incNextKeyNum();
    }

    public void insertToHeadAndLeftChild(KeyLocation indexedKeyLocation, long leftChildPageId) {
        ByteBufferUtils.shiftRight(keysAndChildrenBB, 0, TypeSize.LONG + KeyLocation.SIZE);
        seekToLeftChildOfKey(0);
        setChild(leftChildPageId);
        setKey(indexedKeyLocation);
        incNextKeyNum();
    }

    public void appendKeyAndLeftChild(KeyLocation indexedKeyLocation, long leftChildPageId) {
        seekToLeftChildOfKey(getNextKeyNum());
        setChild(leftChildPageId);
        setKey(indexedKeyLocation);
        incNextKeyNum();
    }

    public void addRightChildToEnd(long rightChildPageId) {
        seekToRightChildOfKey(getNextKeyNum() - 1);
        setChild(rightChildPageId);
    }

    public void addKeyToEnd(KeyLocation indexedKeyLocation) {
        seekToKey(getNextKeyNum());
        setKey(indexedKeyLocation);
        incNextKeyNum();
    }

    public void insertKeyToHead(KeyLocation indexedKeyLocation) {
        ByteBufferUtils.shiftRight(keysAndChildrenBB, 0, TypeSize.LONG + KeyLocation.SIZE);
        seekToKey(0);
        setKey(indexedKeyLocation);
        incNextKeyNum();
    }

    public void addKeyAndRightChildToEnd(KeyLocation indexedKeyLocation, long rightChildPageId) {
        seekToKey(getNextKeyNum());
        setKey(indexedKeyLocation);
        setChild(rightChildPageId);
        incNextKeyNum();
    }

    public long getRightChildOfLastKey() {
        seekToRightChildOfKey(getNextKeyNum() - 1);
        return keysAndChildrenBB.getLong();
    }

    public long getLeftChildOfFirstKey() {
        seekToLeftChildOfKey(0);
        return keysAndChildrenBB.getLong();
    }

//    public long getLeftChild(int keyNum) {
//        seekToLeftChildOfKey(keyNum);
//        return keysAndChildrenBB.getLong();
//    }

    public long getLeftChild(int keyNum) {
//        seekToLeftChildOfKey(keyNum);
        int seekPos = keyNum * (TypeSize.LONG + KeyLocation.SIZE);
//        keysAndChildrenBB.position(bbStartPosition + seekPos);
        return keysAndChildrenBB.getLong(bbStartPosition + seekPos);
    }

    public long getRightChild(int keyNum) {
//        seekToRightChildOfKey(keyNum);
        int seekPos = keyNum * (TypeSize.LONG + KeyLocation.SIZE) + (TypeSize.LONG + KeyLocation.SIZE);
//        keysAndChildrenBB.position(bbStartPosition + seekPos);
        return keysAndChildrenBB.getLong(bbStartPosition + seekPos);
    }

    public int getNextKeyNum() {
//        nextKeyBB.reset();
//        try {
//            return nextKeyBB.getInt();
//        } finally {
//            nextKeyBB.reset();
//        }
        return nextKeyPos;
    }

    public boolean isDeficient() {
        return getNextKeyNum() < btreeConf.minKeys;
    }

    public boolean isAlmostDeficient() {
        return getNextKeyNum() <= btreeConf.minKeys;
    }

    public boolean isLendable() {
        return getNextKeyNum() > btreeConf.minKeys;
    }

    public boolean isMergeable() {
        return getNextKeyNum() ==  btreeConf.minKeys;
    }

    public int getKeyNumOfLeftChild(long leftChildPageId) {
        for (int i = 0; i < getNextKeyNum(); i++) {
            if (getLeftChild(i) == leftChildPageId) {
                return i;
            }
        }
        throw new RuntimeException("Cannot find pos of child: " + leftChildPageId);
    }

    public int getKeyNumOfRightChild(long rightChildPageId) {
        for (int i = 0; i < getNextKeyNum(); i++) {
            if (getRightChild(i) == rightChildPageId) {
                return i;
            }
        }
        throw new RuntimeException("Cannot find pos of child: " + rightChildPageId);
    }

    public void replaceWithNewKey(int keyNum, KeyLocation location) {
        seekToKey(keyNum);
        KeyLocation oldKeyLocation = getKeyLocation(keyNum);
        keysCache.remove(oldKeyLocation);
        setKey(location);

    }

    private void setChild(long pageId) {
        keysAndChildrenBB.putLong(pageId);
    }

    private void setKey(KeyLocation indexedKeyLocation) {
        keysAndChildrenBB.putLong(indexedKeyLocation.pageId);
        keysAndChildrenBB.putShort(indexedKeyLocation.slotId);
    }

    private void seekToKey(int keyNum) {
        int seekPos = TypeSize.LONG + keyNum * (TypeSize.LONG + KeyLocation.SIZE);
        keysAndChildrenBB.position(bbStartPosition + seekPos);
    }

    private void seekToLeftChildOfKey(int keyNum) {
        int seekPos = keyNum * (TypeSize.LONG + KeyLocation.SIZE);
        keysAndChildrenBB.position(bbStartPosition + seekPos);
    }

    private void seekToRightChildOfKey(int keyNum) {
        int seekPos = keyNum * (TypeSize.LONG + KeyLocation.SIZE) + (TypeSize.LONG + KeyLocation.SIZE);
        keysAndChildrenBB.position(bbStartPosition + seekPos);
    }

    private void setNextKeyNum(int num) {
        nextKeyBB.reset();
        nextKeyBB.putInt(num);
        nextKeyBB.reset();
        nextKeyPos = num;
    }

    private void incNextKeyNum() {
        nextKeyBB.reset();
        int x = nextKeyBB.getInt();
        nextKeyBB.reset();
        nextKeyBB.putInt(x + 1);
        nextKeyBB.reset();
        nextKeyPos++;
    }

    private void decNextKeyNum() {
        nextKeyBB.reset();
        int x = nextKeyBB.getInt();
        nextKeyBB.reset();
        nextKeyBB.putInt(x - 1);
        nextKeyBB.reset();
        nextKeyPos--;
    }

    public String toString1() {
        StringBuilder sb = new StringBuilder();
        keysAndChildrenBB.reset();
        sb.append(keysAndChildrenBB.getLong());
        for (int i = 0; i < btreeConf.leafNodeKVArrSize; i++) {
            sb.append("[").append(keysAndChildrenBB.getLong()).append(",").append(keysAndChildrenBB.getShort()).append("]");
            sb.append(keysAndChildrenBB.getLong());
        }
        return sb.toString();
    }

    public String toString2(IndexPageManager indexPageManager) throws IOException {
        StringBuilder sb = new StringBuilder();
        keysAndChildrenBB.reset();
        sb.append(keysAndChildrenBB.getLong());
        for (int i = 0; i < getNextKeyNum(); i++) {
//            sb.append("[").append(keysAndChildrenBB.getLong()).append(",").append(keysAndChildrenBB.getShort()).append("]");
            KeyLocation keyLocation = new KeyLocation(keysAndChildrenBB.getLong(), keysAndChildrenBB.getShort());
            sb.append("[").append(keyLocation.pageId).append(",").append(keyLocation.slotId).append(":").append(indexPageManager.getKey(keyLocation)).append("]");
            sb.append(keysAndChildrenBB.getLong());
        }
        return sb.toString();
    }

}
