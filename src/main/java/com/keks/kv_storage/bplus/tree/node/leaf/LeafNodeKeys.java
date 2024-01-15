package com.keks.kv_storage.bplus.tree.node.leaf;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.tree.node.KeysBinarySearch;
import com.keks.kv_storage.bplus.utils.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;


public class LeafNodeKeys extends KeysBinarySearch<LeafDataLocation> {

    private final ByteBuffer nextKeyBB;
    private volatile int nextKeyPos;
    private final ByteBuffer keysBB;
    private final BtreeConf btreeParams;
    private final int bbStartPosition;

    private final ConcurrentHashMap<LeafDataLocation, String> keysCache;

    public LeafNodeKeys(ByteBuffer nextKeyBB,
                        ByteBuffer keysBB,
                        BtreeConf btreeParams,
                        LeafDataLocation[] keys) {
        this(nextKeyBB, keysBB, btreeParams, keys, new ConcurrentHashMap<>());
    }

    public LeafNodeKeys(ByteBuffer nextKeyBB,
                        ByteBuffer keysBB,
                        BtreeConf btreeParams,
                        ConcurrentHashMap<LeafDataLocation, String> keysCache) {
        this(nextKeyBB, keysBB, btreeParams, new LeafDataLocation[]{}, keysCache);
    }

    public LeafNodeKeys(ByteBuffer nextKeyBB,
                        ByteBuffer keysBB,
                        BtreeConf btreeParams,
                        LeafDataLocation[] keys,
                        ConcurrentHashMap<LeafDataLocation, String> keysCache) {
        this.nextKeyBB = nextKeyBB;
        nextKeyBB.reset();
        this.nextKeyPos = nextKeyBB.getInt();
        this.keysBB = keysBB;
        this.btreeParams = btreeParams;
        keysBB.reset();
        this.bbStartPosition = keysBB.position();
        for (LeafDataLocation key : keys) {
            setKey(key);
            incNextKeyNum();
        }
        this.keysCache = keysCache;

    }

    public void moveRightHalfToNewRightNode(LeafNodeKeys rightNode) {
        int firstKeyNum = btreeParams.keysArrMidPointPos;
        seekToKey(firstKeyNum);
        rightNode.seekToKey(0);
        int cnt = 0;
        for (; firstKeyNum < btreeParams.leafNodeKVArrSize; ++firstKeyNum) {
            LeafDataLocation keyLocation = getKeyLocation(firstKeyNum);
            keysCache.remove(keyLocation);
            rightNode.setKey(keyLocation);
            cnt++;
        }
        rightNode.setNextKeyNum(cnt);
        setNextKeyNum(btreeParams.keysArrMidPointPos);
    }

    public void addRightNode(LeafNodeKeys rightNode) {
        seekToKey(getNextKeyNum());
        int cnt = 0;
        for (int i = 0; i < rightNode.getNextKeyNum(); i++) {
            setKey(rightNode.getKeyLocation(i));
            cnt++;
        }
        setNextKeyNum(getNextKeyNum() + cnt);
        rightNode.setNextKeyNum(0);
        rightNode.keysCache.clear();
    }

    public void addLeftNode(LeafNodeKeys leftNode) {
        int leftNodeKeys = leftNode.getNextKeyNum();
        ByteBufferUtils.shiftRight(keysBB, 0, leftNodeKeys * LeafDataLocation.SIZE);
        for (int i = 0; i < leftNodeKeys; i++) {
            seekToKey(i);
            setKey(leftNode.getKeyLocation(i));
            incNextKeyNum();
        }
        leftNode.setNextKeyNum(0);
        leftNode.keysCache.clear();
    }


    public LeafDataLocation getMidKeyDataLocation() {
        seekToKey(btreeParams.keysArrMidPointPos);
        return new LeafDataLocation(keysBB.getLong(), keysBB.getShort());
    }

    public LeafDataLocation getKeyLocation(int keyNum) {
        if (keyNum >= getNextKeyNum())
            throw new IllegalArgumentException("NodeKey should be lower then: " + getNextKeyNum());
        int seekPos = keyNum * LeafDataLocation.SIZE;
//        keysBB.position(bbStartPosition + seekPos);
        return new LeafDataLocation(keysBB.getLong(bbStartPosition + seekPos), keysBB.getShort(bbStartPosition + seekPos + TypeSize.LONG));
    }

    public LeafDataLocation delete(int keyNum) {
        LeafDataLocation deleted = getKeyLocation(keyNum);
        keysCache.remove(deleted);
        int srcPos = keyNum * LeafDataLocation.SIZE;
        ByteBufferUtils.shiftLeft(keysBB, srcPos + LeafDataLocation.SIZE, LeafDataLocation.SIZE);
        decNextKeyNum();
        return deleted;
    }

    public void replaceWithNewKey(int keyNum, LeafDataLocation location) {
        seekToKey(keyNum);
        LeafDataLocation oldKeyLocation = getKeyLocation(keyNum);
        keysCache.remove(oldKeyLocation);
        setKey(location);
    }

    public void insertNewKey(int keyNum, LeafDataLocation indexedKeyLocation) {
        if (keyNum > getNextKeyNum()) throw new IllegalArgumentException("NodeKey");
        if (keyNum != getNextKeyNum()) {
            ByteBufferUtils.shiftRight(keysBB, keyNum * LeafDataLocation.SIZE, LeafDataLocation.SIZE);
        }
        seekToKey(keyNum);
        setKey(indexedKeyLocation);
        incNextKeyNum();

    }

    public boolean isFull() {
        return getNextKeyNum() == btreeParams.leafNodeKVArrSize;
    }

    public boolean isAlmostFull() {
        return getNextKeyNum() == btreeParams.leafNodeKVArrSize - 1;
    }

    public boolean isEmpty() {
        return getNextKeyNum() == 0;
    }

    public boolean isAlmostEmpty() {
        return getNextKeyNum() == 1;
    }

    public int getNextKeyNum() {
//        nextKeyBB.reset();
//        return nextKeyBB.getInt();
        return nextKeyPos;
    }

    public boolean isDeficient() {
        return getNextKeyNum() < btreeParams.minKeys;
    }

    public boolean isAlmostDeficient() {
        return getNextKeyNum() <= btreeParams.minKeys;
    }

    public boolean isLendable() {
        return getNextKeyNum() > btreeParams.minKeys;
    }

    public boolean isMergeable() {
        return getNextKeyNum() == btreeParams.minKeys;
    }

    public LeafDataLocation removeLast() {
        LeafDataLocation deleted = getKeyLocation(getNextKeyNum() - 1);
        keysCache.remove(deleted);
        decNextKeyNum();
        return deleted;
    }

    public LeafDataLocation removeFirst() {
        return delete(0);
    }

    public void addHead(LeafDataLocation leafDataLocation) {
        insertNewKey(0, leafDataLocation);
    }

    public void addTail(LeafDataLocation leafDataLocation) {
        seekToKey(getNextKeyNum());
        setKey(leafDataLocation);
        incNextKeyNum();
    }

    public LeafDataLocation getHead() {
        return getKeyLocation(0);
    }

    public LeafDataLocation getLast() {
        return getKeyLocation(getNextKeyNum() - 1);
    }

    private void setKey(LeafDataLocation keyLocation) {
        keysBB.putLong(keyLocation.pageId);
        keysBB.putShort(keyLocation.slotId);
    }

    private void setNextKeyNum(int num) {
        nextKeyBB.reset();
        nextKeyBB.putInt(num);
        nextKeyPos = num;
    }

    private void incNextKeyNum() {
        nextKeyBB.reset();
        int x = nextKeyBB.getInt();
        nextKeyBB.reset();
        nextKeyBB.putInt(x + 1);
        nextKeyPos++;
    }

    private void decNextKeyNum() {
        nextKeyBB.reset();
        int x = nextKeyBB.getInt();
        nextKeyBB.reset();
        nextKeyBB.putInt(x - 1);
        nextKeyPos--;
    }

    private void seekToKey(int keyNum) {
        try {
            int seekPos = keyNum * LeafDataLocation.SIZE;
            keysBB.position(bbStartPosition + seekPos);
        } catch (Throwable t) {
            System.out.println("KeyNum: " + keyNum + " TreeOrder: " + btreeParams.treeOrder);
            throw t;
        }

    }


    public String toString1() {
        keysBB.reset();
        final StringBuffer sb = new StringBuffer("LeafKeys(");
        for (int i = 0; i < btreeParams.leafNodeKVArrSize; i++) {
            if (i < getNextKeyNum()) {
                sb.append(new LeafDataLocation(keysBB));
            } else {
                sb.append(new LeafDataLocation(0, (short) 0));
            }
            sb.append(", ");

        }
        sb.append(')');
        keysBB.reset();
        return sb.toString();
    }

}
