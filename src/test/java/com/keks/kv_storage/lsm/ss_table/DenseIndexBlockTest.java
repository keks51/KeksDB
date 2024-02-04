package com.keks.kv_storage.lsm.ss_table;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class DenseIndexBlockTest {

    @Test
    public void test1() {
        IndexedKey key1 = new IndexedKey("key1", 0, 1);
        DenseIndexBlock expDenseIndexBlock = new DenseIndexBlock.DenseIndexBlockBuilder(1).add(key1).build();

        byte[] bytes = expDenseIndexBlock.getBytes();

        DenseIndexBlock actDenseIndexBlock = DenseIndexBlock.fromByteBuffer(ByteBuffer.wrap(bytes));

        assertEquals(expDenseIndexBlock, actDenseIndexBlock);
        assertEquals(key1, actDenseIndexBlock.getKey(0));
        assertEquals(1, actDenseIndexBlock.getKeysNum());
    }

    @Test
    public void test2() {
        int indexBlockSize = 8192;
        ArrayList<IndexedKey> expKeys = new ArrayList<>();
        for (int i = 0; i < indexBlockSize; i++) {
            IndexedKey kvRecord = new IndexedKey("key" + i, i, i * 2);
            expKeys.add(kvRecord);
        }
        Collections.sort(expKeys, Comparator.comparing(o -> o.key));
        DenseIndexBlock.DenseIndexBlockBuilder denseIndexBlockBuilder = new DenseIndexBlock.DenseIndexBlockBuilder(indexBlockSize);
        for (IndexedKey expKey : expKeys) {
            denseIndexBlockBuilder.add(expKey);
        }
        DenseIndexBlock expDenseIndexBlock = denseIndexBlockBuilder.build();

        byte[] bytes = expDenseIndexBlock.getBytes();
        DenseIndexBlock actDenseIndexBlock = DenseIndexBlock.fromByteBuffer(ByteBuffer.wrap(bytes));

        assertEquals(indexBlockSize, actDenseIndexBlock.getKeysNum());
        assertEquals(expDenseIndexBlock, actDenseIndexBlock);
        for (int i = 0; i < actDenseIndexBlock.getKeysNum(); i++) {
            IndexedKey expKey = expKeys.get(i);
            IndexedKey actKey = actDenseIndexBlock.getKey(i);
            assertEquals(expKey, actKey);
            assertEquals(actKey, actDenseIndexBlock.findKey(expKey.key));
        }

    }

    @Test
    public void test3() {
        for (int indexBlockSize = 0; indexBlockSize < 3000; indexBlockSize++) {
            ArrayList<IndexedKey> expKeys = new ArrayList<>();
            for (int i = 0; i < indexBlockSize; i++) {
                IndexedKey kvRecord = new IndexedKey("key" + i, i, i * 2);
                expKeys.add(kvRecord);
            }
            Collections.sort(expKeys, Comparator.comparing(o -> o.key));
            DenseIndexBlock.DenseIndexBlockBuilder denseIndexBlockBuilder = new DenseIndexBlock.DenseIndexBlockBuilder(indexBlockSize);
            for (IndexedKey expKey : expKeys) {
                denseIndexBlockBuilder.add(expKey);
            }
            DenseIndexBlock expDenseIndexBlock = denseIndexBlockBuilder.build();

            byte[] bytes = expDenseIndexBlock.getBytes();
            DenseIndexBlock actDenseIndexBlock = DenseIndexBlock.fromByteBuffer(ByteBuffer.wrap(bytes));

            assertEquals(indexBlockSize, actDenseIndexBlock.getKeysNum());
            assertEquals(expDenseIndexBlock, actDenseIndexBlock);
            for (int i = 0; i < actDenseIndexBlock.getKeysNum(); i++) {
                IndexedKey expKey = expKeys.get(i);
                IndexedKey actKey = actDenseIndexBlock.getKey(i);
                assertEquals(expKey, actKey);
                assertEquals(actKey, actDenseIndexBlock.findKey(expKey.key));
            }
        }
    }

    @Test
    public void test4() {
        for (int indexBlockSize = 0; indexBlockSize < 3000; indexBlockSize++) {
            ArrayList<IndexedKey> expKeys = new ArrayList<>();
            for (int i = 0; i < indexBlockSize; i++) {
                IndexedKey kvRecord = new IndexedKey("key" + i, i, i * 2);
                expKeys.add(kvRecord);
            }
            Collections.sort(expKeys, Comparator.comparing(o -> o.key));
            DenseIndexBlock.DenseIndexBlockBuilder denseIndexBlockBuilder = new DenseIndexBlock.DenseIndexBlockBuilder(indexBlockSize + new Random().nextInt(100));
            for (IndexedKey expKey : expKeys) {
                denseIndexBlockBuilder.add(expKey);
            }
            DenseIndexBlock expDenseIndexBlock = denseIndexBlockBuilder.build();

            byte[] bytes = expDenseIndexBlock.getBytes();
            DenseIndexBlock actDenseIndexBlock = DenseIndexBlock.fromByteBuffer(ByteBuffer.wrap(bytes));

            assertEquals(indexBlockSize, actDenseIndexBlock.getKeysNum());
            assertEquals(expDenseIndexBlock, actDenseIndexBlock);
            for (int i = 0; i < actDenseIndexBlock.getKeysNum(); i++) {
                IndexedKey expKey = expKeys.get(i);
                IndexedKey actKey = actDenseIndexBlock.getKey(i);
                assertEquals(expKey, actKey);
                assertEquals(actKey, actDenseIndexBlock.findKey(expKey.key));
            }
        }
    }

    @Test
    public void test5() {
        IndexedKey key1 = new IndexedKey("key1", 0, 1);
        DenseIndexBlock expDenseIndexBlock = new DenseIndexBlock.DenseIndexBlockBuilder(2).add(key1).build();

        byte[] bytes = expDenseIndexBlock.getBytes();

        DenseIndexBlock actDenseIndexBlock = DenseIndexBlock.fromByteBuffer(ByteBuffer.wrap(bytes));

        assertEquals(expDenseIndexBlock, actDenseIndexBlock);
        assertEquals(key1, actDenseIndexBlock.getKey(0));
        assertEquals(1, actDenseIndexBlock.getKeysNum());
    }

    @Test
    public void test6() {
        IndexedKey key1 = new IndexedKey("key1", 0, 1);
        DenseIndexBlock expDenseIndexBlock = new DenseIndexBlock.DenseIndexBlockBuilder(1).add(key1).build();

        assertNull(expDenseIndexBlock.findKey("key0"));
        assertEquals("key1", expDenseIndexBlock.findKey("key1").key);
        assertNull(expDenseIndexBlock.findKey("key2"));
    }

}