package com.keks.kv_storage.bplus.tree.node.key;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.bplus.conf.BtreeConf;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;


class KeyNodeKeysTest {

    @Test
    public void test1() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        assertEquals(0, keysAndChildren.getNextKeyNum());

        keysAndChildren.bulkInsertInEmptyNode(new KeyLocation[]{new KeyLocation(0, (short) 0), new KeyLocation(1, (short) 1)}, new long[]{3, 4, 5});

        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(new KeyLocation(1, (short) 1), keysAndChildren.getKeyLocation(1));
        assertThrows(IllegalArgumentException.class, () -> keysAndChildren.getKeyLocation(2));

//        System.out.println(keysAndChildren.toString1());
    }

    // bulk insert and insert one Key
    @Test
    public void test2() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.bulkInsertInEmptyNode(
                new KeyLocation[]{
                        new KeyLocation(0, (short) 0),
                        new KeyLocation(3, (short) 3),
                        new KeyLocation(5, (short) 5),
                        new KeyLocation(7, (short) 7),
                        new KeyLocation(9, (short) 9)},
                new long[]{33, 55, 77, 99, 111, 333});
        assertFalse(keysAndChildren.isFull());
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
        assertEquals(new KeyLocation(7, (short) 7), keysAndChildren.getKeyLocation(3));
        assertEquals(new KeyLocation(9, (short) 9), keysAndChildren.getKeyLocation(4));

        keysAndChildren.insertKeyAndRightChild(1, new KeyLocation(2, (short) 2), 66);

        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(new KeyLocation(2, (short) 2), keysAndChildren.getKeyLocation(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(2));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(3));
        assertEquals(new KeyLocation(7, (short) 7), keysAndChildren.getKeyLocation(4));
        assertEquals(new KeyLocation(9, (short) 9), keysAndChildren.getKeyLocation(5));
        assertEquals(55L, keysAndChildren.getLeftChild(1));
        assertEquals(66L, keysAndChildren.getRightChild(1));
    }

    // addKeyToEnd
    @Test
    public void test3() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.bulkInsertInEmptyNode(
                new KeyLocation[]{
                        new KeyLocation(0, (short) 0),
                        new KeyLocation(3, (short) 3),
                        new KeyLocation(5, (short) 5)},
                new long[]{33, 55, 77, 99});

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));

        assertEquals(77, keysAndChildren.getLeftChild(2));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
        assertEquals(99, keysAndChildren.getRightChild(2));

        keysAndChildren.addKeyAndRightChildToEnd(new KeyLocation(7, (short) 7), 111);

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));

        assertEquals(77, keysAndChildren.getLeftChild(2));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
        assertEquals(99, keysAndChildren.getRightChild(2));

        assertEquals(99, keysAndChildren.getLeftChild(3));
        assertEquals(new KeyLocation(7, (short) 7), keysAndChildren.getKeyLocation(3));
        assertEquals(111, keysAndChildren.getRightChild(3));

    }

    // joinWithRightNode
    @Test
    public void test4() {
        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(2, (short) 2)},
                    new long[]{10, 11, 12, 13});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(6, (short) 6)},
                    new long[]{14, 15, 16, 17});

            leftKeysAndChildren.addKeyAndRightChildToEnd(new KeyLocation(3, (short) 3), 19);
            assertFalse(leftKeysAndChildren.isFull());

            leftKeysAndChildren.joinWithRightNode(rightKeysAndChildren);

            assertTrue(leftKeysAndChildren.isFull());
            assertEquals(10, leftKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), leftKeysAndChildren.getKeyLocation(0));
            assertEquals(11, leftKeysAndChildren.getRightChild(0));

            assertEquals(11, leftKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), leftKeysAndChildren.getKeyLocation(1));
            assertEquals(12, leftKeysAndChildren.getRightChild(1));

            assertEquals(12, leftKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(2, (short) 2), leftKeysAndChildren.getKeyLocation(2));
            assertEquals(13, leftKeysAndChildren.getRightChild(2));

            assertEquals(13, leftKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(3, (short) 3), leftKeysAndChildren.getKeyLocation(3));
            assertEquals(14, leftKeysAndChildren.getRightChild(3));

            assertEquals(14, leftKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(4, (short) 4), leftKeysAndChildren.getKeyLocation(4));
            assertEquals(15, leftKeysAndChildren.getRightChild(4));

            assertEquals(15, leftKeysAndChildren.getLeftChild(5));
            assertEquals(new KeyLocation(5, (short) 5), leftKeysAndChildren.getKeyLocation(5));
            assertEquals(16, leftKeysAndChildren.getRightChild(5));

            assertEquals(16, leftKeysAndChildren.getLeftChild(6));
            assertEquals(new KeyLocation(6, (short) 6), leftKeysAndChildren.getKeyLocation(6));
            assertEquals(17, leftKeysAndChildren.getRightChild(6));

            assertEquals(7, leftKeysAndChildren.getNextKeyNum());
            assertEquals(0, rightKeysAndChildren.getNextKeyNum());
        }

        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(2, (short) 2)},
                    new long[]{10, 11, 12, 13});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(6, (short) 6)},
                    new long[]{14, 15, 16});

            leftKeysAndChildren.addKeyAndRightChildToEnd(new KeyLocation(3, (short) 3), 14);
            assertFalse(leftKeysAndChildren.isFull());

            leftKeysAndChildren.joinWithRightNode(rightKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(10, leftKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), leftKeysAndChildren.getKeyLocation(0));
            assertEquals(11, leftKeysAndChildren.getRightChild(0));

            assertEquals(11, leftKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), leftKeysAndChildren.getKeyLocation(1));
            assertEquals(12, leftKeysAndChildren.getRightChild(1));

            assertEquals(12, leftKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(2, (short) 2), leftKeysAndChildren.getKeyLocation(2));
            assertEquals(13, leftKeysAndChildren.getRightChild(2));

            assertEquals(13, leftKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(3, (short) 3), leftKeysAndChildren.getKeyLocation(3));
            assertEquals(14, leftKeysAndChildren.getRightChild(3));

            assertEquals(14, leftKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(5, (short) 5), leftKeysAndChildren.getKeyLocation(4));
            assertEquals(15, leftKeysAndChildren.getRightChild(4));

            assertEquals(15, leftKeysAndChildren.getLeftChild(5));
            assertEquals(new KeyLocation(6, (short) 6), leftKeysAndChildren.getKeyLocation(5));
            assertEquals(16, leftKeysAndChildren.getRightChild(5));

            assertEquals(6, leftKeysAndChildren.getNextKeyNum());
        }

        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(2, (short) 2)},
                    new long[]{10, 11, 12, 13});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5)},
                    new long[]{14, 15, 16});

            leftKeysAndChildren.addKeyAndRightChildToEnd(new KeyLocation(3, (short) 3), 14);

            leftKeysAndChildren.joinWithRightNode(rightKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(10, leftKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), leftKeysAndChildren.getKeyLocation(0));
            assertEquals(11, leftKeysAndChildren.getRightChild(0));

            assertEquals(11, leftKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), leftKeysAndChildren.getKeyLocation(1));
            assertEquals(12, leftKeysAndChildren.getRightChild(1));

            assertEquals(12, leftKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(2, (short) 2), leftKeysAndChildren.getKeyLocation(2));
            assertEquals(13, leftKeysAndChildren.getRightChild(2));

            assertEquals(13, leftKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(3, (short) 3), leftKeysAndChildren.getKeyLocation(3));
            assertEquals(14, leftKeysAndChildren.getRightChild(3));

            assertEquals(14, leftKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(4, (short) 4), leftKeysAndChildren.getKeyLocation(4));
            assertEquals(15, leftKeysAndChildren.getRightChild(4));

            assertEquals(15, leftKeysAndChildren.getLeftChild(5));
            assertEquals(new KeyLocation(5, (short) 5), leftKeysAndChildren.getKeyLocation(5));
            assertEquals(16, leftKeysAndChildren.getRightChild(5));

            assertEquals(6, leftKeysAndChildren.getNextKeyNum());
        }

        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1)},
                    new long[]{10, 11, 12});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5)},
                    new long[]{14, 15, 16});

            leftKeysAndChildren.addKeyAndRightChildToEnd(new KeyLocation(3, (short) 3), 14);

            leftKeysAndChildren.joinWithRightNode(rightKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(10, leftKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), leftKeysAndChildren.getKeyLocation(0));
            assertEquals(11, leftKeysAndChildren.getRightChild(0));

            assertEquals(11, leftKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), leftKeysAndChildren.getKeyLocation(1));
            assertEquals(12, leftKeysAndChildren.getRightChild(1));

            assertEquals(12, leftKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(3, (short) 3), leftKeysAndChildren.getKeyLocation(2));
            assertEquals(14, leftKeysAndChildren.getRightChild(2));

            assertEquals(14, leftKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(4, (short) 4), leftKeysAndChildren.getKeyLocation(3));
            assertEquals(15, leftKeysAndChildren.getRightChild(3));

            assertEquals(15, leftKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(5, (short) 5), leftKeysAndChildren.getKeyLocation(4));
            assertEquals(16, leftKeysAndChildren.getRightChild(4));

            assertEquals(5, leftKeysAndChildren.getNextKeyNum());
        }
    }

    // removeLastKey
    @Test
    public void test5() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.bulkInsertInEmptyNode(
                new KeyLocation[]{
                        new KeyLocation(0, (short) 0),
                        new KeyLocation(3, (short) 3),
                        new KeyLocation(5, (short) 5)},
                new long[]{33, 55, 77, 99});

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));

        assertEquals(77, keysAndChildren.getLeftChild(2));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
        assertEquals(99, keysAndChildren.getRightChild(2));

        assertEquals(3, keysAndChildren.getNextKeyNum());

        KeyLocationAndRightChild removedKeyLocation = keysAndChildren.removeLastKeyAndRightChild();
        assertEquals(new KeyLocationAndRightChild(new KeyLocation(5, (short) 5), 99L), removedKeyLocation);

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));


        assertEquals(2, keysAndChildren.getNextKeyNum());
    }

    // removeKeyAndRightChild
    @Test
    public void test6() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.bulkInsertInEmptyNode(
                new KeyLocation[]{
                        new KeyLocation(0, (short) 0),
                        new KeyLocation(3, (short) 3),
                        new KeyLocation(5, (short) 5)},
                new long[]{33, 55, 77, 99});

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));

        assertEquals(77, keysAndChildren.getLeftChild(2));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
        assertEquals(99, keysAndChildren.getRightChild(2));

        assertEquals(3, keysAndChildren.getNextKeyNum());

        keysAndChildren.removeKeyAndRightChild(1);

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(1));
        assertEquals(99, keysAndChildren.getRightChild(1));

        assertEquals(2, keysAndChildren.getNextKeyNum());

        keysAndChildren.removeKeyAndRightChild(1);

        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(1, keysAndChildren.getNextKeyNum());

        keysAndChildren.removeKeyAndRightChild(0);
        assertEquals(0, keysAndChildren.getNextKeyNum());
    }

    // remove first key
    @Test
    public void test7() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.bulkInsertInEmptyNode(
                new KeyLocation[]{
                        new KeyLocation(0, (short) 0),
                        new KeyLocation(3, (short) 3),
                        new KeyLocation(5, (short) 5)},
                new long[]{33, 55, 77, 99});

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));

        assertEquals(77, keysAndChildren.getLeftChild(2));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
        assertEquals(99, keysAndChildren.getRightChild(2));

        assertEquals(3, keysAndChildren.getNextKeyNum());

        KeyLocationAndLeftChild removedKeyLocation = keysAndChildren.removeFirstKeyAndLeftChild();
        assertEquals(new KeyLocationAndLeftChild(new KeyLocation(0, (short) 0), 33L), removedKeyLocation);

        assertFalse(keysAndChildren.isFull());
        assertEquals(55, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(0));
        assertEquals(77, keysAndChildren.getRightChild(0));

        assertEquals(77, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(1));
        assertEquals(99, keysAndChildren.getRightChild(1));


        assertEquals(2, keysAndChildren.getNextKeyNum());
    }

    // addToHead
    @Test
    public void test8() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.bulkInsertInEmptyNode(
                new KeyLocation[]{
                        new KeyLocation(1, (short) 1),
                        new KeyLocation(3, (short) 3)},
                new long[]{33, 55, 77});

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(1, (short) 1), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));

        assertEquals(2, keysAndChildren.getNextKeyNum());

        keysAndChildren.insertToHeadAndLeftChild(new KeyLocation(0, (short) 0), 11);

        assertFalse(keysAndChildren.isFull());

        assertEquals(11, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(33, keysAndChildren.getRightChild(0));

        assertEquals(33, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(1, (short) 1), keysAndChildren.getKeyLocation(1));
        assertEquals(55, keysAndChildren.getRightChild(1));

        assertEquals(55, keysAndChildren.getLeftChild(2));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(2));
        assertEquals(77, keysAndChildren.getRightChild(2));


        assertEquals(3, keysAndChildren.getNextKeyNum());
    }

    // getMidKey
    @Test
    public void test9() {
        {
            BtreeConf btreeParams = new BtreeConf(3);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(5, (short) 5)},
                    new long[]{33, 55, 77, 99});

            assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getMidIndex());
        }
        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5)},
                    new long[]{33, 44, 55, 77, 99});

            assertEquals(new KeyLocation(4, (short) 4), keysAndChildren.getMidIndex());
        }
        {
            BtreeConf btreeParams = new BtreeConf(5);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(6, (short) 6)},
                    new long[]{33, 44, 55, 66, 77, 99});

            assertEquals(new KeyLocation(4, (short) 4), keysAndChildren.getMidIndex());
        }
    }

    // moveRightHalfToRightNode
    @Test
    public void test10() {
        {
            BtreeConf btreeParams = new BtreeConf(3);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(5, (short) 5)},
                    new long[]{33, 55, 77, 99});
            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            assertTrue(leftKeysAndChildren.isFull());

            leftKeysAndChildren.moveRightHalfToNewRightNode(rightKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(33, leftKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), leftKeysAndChildren.getKeyLocation(0));
            assertEquals(55, leftKeysAndChildren.getRightChild(0));

            assertEquals(1, leftKeysAndChildren.getNextKeyNum());


            assertFalse(rightKeysAndChildren.isFull());
            assertEquals(77, rightKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(5, (short) 5), rightKeysAndChildren.getKeyLocation(0));
            assertEquals(99, rightKeysAndChildren.getRightChild(0));

            assertEquals(1, rightKeysAndChildren.getNextKeyNum());

        }
        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5)},
                    new long[]{33, 44, 55, 77, 99});
            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            assertTrue(leftKeysAndChildren.isFull());

            leftKeysAndChildren.moveRightHalfToNewRightNode(rightKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(33, leftKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), leftKeysAndChildren.getKeyLocation(0));
            assertEquals(44, leftKeysAndChildren.getRightChild(0));

            assertEquals(44, leftKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(3, (short) 3), leftKeysAndChildren.getKeyLocation(1));
            assertEquals(55, leftKeysAndChildren.getRightChild(1));

            assertEquals(2, leftKeysAndChildren.getNextKeyNum());


            assertFalse(rightKeysAndChildren.isFull());
            assertEquals(77, rightKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(5, (short) 5), rightKeysAndChildren.getKeyLocation(0));
            assertEquals(99, rightKeysAndChildren.getRightChild(0));

            assertEquals(1, rightKeysAndChildren.getNextKeyNum());
        }
        {
            BtreeConf btreeParams = new BtreeConf(5);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(6, (short) 6)},
                    new long[]{33, 44, 55, 66, 77, 99});
            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            assertTrue(leftKeysAndChildren.isFull());

            leftKeysAndChildren.moveRightHalfToNewRightNode(rightKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(33, leftKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), leftKeysAndChildren.getKeyLocation(0));
            assertEquals(44, leftKeysAndChildren.getRightChild(0));

            assertEquals(44, leftKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(3, (short) 3), leftKeysAndChildren.getKeyLocation(1));
            assertEquals(55, leftKeysAndChildren.getRightChild(1));

            assertEquals(2, leftKeysAndChildren.getNextKeyNum());


            assertFalse(rightKeysAndChildren.isFull());
            assertEquals(66, rightKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(5, (short) 5), rightKeysAndChildren.getKeyLocation(0));
            assertEquals(77, rightKeysAndChildren.getRightChild(0));

            assertEquals(77, rightKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(6, (short) 6), rightKeysAndChildren.getKeyLocation(1));
            assertEquals(99, rightKeysAndChildren.getRightChild(1));

            assertEquals(2, rightKeysAndChildren.getNextKeyNum());
        }
    }

    // insertKeyAndChildren
    @Test
    public void test11() {
        BtreeConf btreeParams = new BtreeConf(4);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.insertKeyAndChildrenWhenEmpty(new KeyLocation(1, (short) 3), 11, 12);

        assertEquals(11, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(1, (short) 3), keysAndChildren.getKeyLocation(0));
        assertEquals(12, keysAndChildren.getRightChild(0));
        assertEquals(1, keysAndChildren.getNextKeyNum());
    }

    // isDeficient, isMergeable, isLendable
    @Test
    public void test12() {
        // tree order = 3
        {
            BtreeConf btreeParams = new BtreeConf(3);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3)},
                    new long[]{33, 55, 77});

            assertFalse(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isMergeable());
            assertTrue(keysAndChildren.isLendable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(3);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0)},
                    new long[]{33, 55});

            assertFalse(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isLendable());
            assertTrue(keysAndChildren.isMergeable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(3);
            KeyNodeKeys keysAndChildren = create(btreeParams);

            assertTrue(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isLendable());
            assertFalse(keysAndChildren.isMergeable());
        }


        // tree order = 4
        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(4, (short) 4)},
                    new long[]{33, 55, 77, 88});

            assertFalse(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isMergeable());
            assertTrue(keysAndChildren.isLendable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3)},
                    new long[]{33, 55, 66});

            assertFalse(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isMergeable());
            assertTrue(keysAndChildren.isLendable());

        }

        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0)},
                    new long[]{33, 55});

            assertFalse(keysAndChildren.isDeficient());
            assertTrue(keysAndChildren.isMergeable());
            assertFalse(keysAndChildren.isLendable());

        }

        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);

            assertTrue(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isLendable());
            assertFalse(keysAndChildren.isMergeable());
        }


        // tree order = 5
        {
            BtreeConf btreeParams = new BtreeConf(5);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(4, (short) 4)},
                    new long[]{33, 55, 77, 88});

            assertFalse(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isMergeable());
            assertTrue(keysAndChildren.isLendable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(5);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3)},
                    new long[]{33, 55, 66});

            assertFalse(keysAndChildren.isDeficient());
            assertTrue(keysAndChildren.isMergeable());
            assertFalse(keysAndChildren.isLendable());

        }

        {
            BtreeConf btreeParams = new BtreeConf(5);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0)},
                    new long[]{33, 55});

            assertTrue(keysAndChildren.isDeficient());
            assertFalse(keysAndChildren.isMergeable());
            assertFalse(keysAndChildren.isLendable());

        }

    }


    // isAlmostFull
    @Test
    public void test13() {
        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(7, (short) 7)},
                    new long[]{33, 55, 77, 99, 111});
            assertFalse(keysAndChildren.isAlmostFull());
        }

        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(5, (short) 5)},
                    new long[]{33, 55, 77, 99});
            assertTrue(keysAndChildren.isAlmostFull());
        }
    }

    // getKeyNumOfLeftChild
    @Test
    public void test14() {
        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(7, (short) 7)},
                    new long[]{33, 55, 77, 99, 111});
            assertEquals(0, keysAndChildren.getKeyNumOfLeftChild(33));
            assertEquals(1, keysAndChildren.getKeyNumOfLeftChild(55));
            assertEquals(2, keysAndChildren.getKeyNumOfLeftChild(77));
            assertEquals(3, keysAndChildren.getKeyNumOfLeftChild(99));
        }
    }

    // replaceWithNewKey
    @Test
    public void test15() {
        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(7, (short) 7)},
                    new long[]{33, 55, 77, 99, 111});

            keysAndChildren.replaceWithNewKey(1, new KeyLocation(22, (short) 22));
            keysAndChildren.replaceWithNewKey(0, new KeyLocation(11, (short) 11));
            keysAndChildren.replaceWithNewKey(3, new KeyLocation(88, (short) 88));

            assertEquals(new KeyLocation(11, (short) 11), keysAndChildren.getKeyLocation(0));
            assertEquals(new KeyLocation(22, (short) 22), keysAndChildren.getKeyLocation(1));
            assertEquals( new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
            assertEquals( new KeyLocation(88, (short) 88), keysAndChildren.getKeyLocation(3));
        }
    }

    // removeKeyAndLeftChild
    @Test
    public void test16() {
        BtreeConf btreeParams = new BtreeConf(6);
        KeyNodeKeys keysAndChildren = create(btreeParams);
        keysAndChildren.bulkInsertInEmptyNode(
                new KeyLocation[]{
                        new KeyLocation(0, (short) 0),
                        new KeyLocation(3, (short) 3),
                        new KeyLocation(5, (short) 5)},
                new long[]{33, 55, 77, 99});

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(55, keysAndChildren.getRightChild(0));

        assertEquals(55, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(1));
        assertEquals(77, keysAndChildren.getRightChild(1));

        assertEquals(77, keysAndChildren.getLeftChild(2));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(2));
        assertEquals(99, keysAndChildren.getRightChild(2));

        assertEquals(3, keysAndChildren.getNextKeyNum());

        keysAndChildren.removeKeyAndLeftChild(1);

        assertFalse(keysAndChildren.isFull());
        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(77, keysAndChildren.getRightChild(0));

        assertEquals(77, keysAndChildren.getLeftChild(1));
        assertEquals(new KeyLocation(5, (short) 5), keysAndChildren.getKeyLocation(1));
        assertEquals(99, keysAndChildren.getRightChild(1));

        assertEquals(2, keysAndChildren.getNextKeyNum());

        keysAndChildren.removeKeyAndLeftChild(1);

        assertEquals(33, keysAndChildren.getLeftChild(0));
        assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
        assertEquals(99, keysAndChildren.getRightChild(0));

        assertEquals(1, keysAndChildren.getNextKeyNum());

        keysAndChildren.removeKeyAndLeftChild(0);
        assertEquals(0, keysAndChildren.getNextKeyNum());
    }

    // getKeyNumOfRightChild
    @Test
    public void test17() {
        {
            BtreeConf btreeParams = new BtreeConf(4);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(3, (short) 3),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(7, (short) 7)},
                    new long[]{33, 55, 77, 99, 111});
            assertEquals(0, keysAndChildren.getKeyNumOfRightChild(55));
            assertEquals(1, keysAndChildren.getKeyNumOfRightChild(77));
            assertEquals(2, keysAndChildren.getKeyNumOfRightChild(99));
            assertEquals(3, keysAndChildren.getKeyNumOfRightChild(111));
        }
    }

    // joinWithLeftNode
    @Test
    public void test18() {
        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(2, (short) 2)},
                    new long[]{10, 11, 12, 13});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(6, (short) 6)},
                    new long[]{14, 15, 16, 17});
//            System.out.println(rightKeysAndChildren.toString1());
            rightKeysAndChildren.insertKeyToHead(new KeyLocation(3, (short) 3));
//            System.out.println(rightKeysAndChildren.toString1());
            assertFalse(leftKeysAndChildren.isFull());

            rightKeysAndChildren.joinWithLeftNode(leftKeysAndChildren);
            System.out.println(rightKeysAndChildren.toString1());

            assertTrue(rightKeysAndChildren.isFull());
            assertEquals(10, rightKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), rightKeysAndChildren.getKeyLocation(0));
            assertEquals(11, rightKeysAndChildren.getRightChild(0));

            assertEquals(11, rightKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), rightKeysAndChildren.getKeyLocation(1));
            assertEquals(12, rightKeysAndChildren.getRightChild(1));

            assertEquals(12, rightKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(2, (short) 2), rightKeysAndChildren.getKeyLocation(2));
            assertEquals(13, rightKeysAndChildren.getRightChild(2));

            assertEquals(13, rightKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(3, (short) 3), rightKeysAndChildren.getKeyLocation(3));
            assertEquals(14, rightKeysAndChildren.getRightChild(3));

            assertEquals(14, rightKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(4, (short) 4), rightKeysAndChildren.getKeyLocation(4));
            assertEquals(15, rightKeysAndChildren.getRightChild(4));

            assertEquals(15, rightKeysAndChildren.getLeftChild(5));
            assertEquals(new KeyLocation(5, (short) 5), rightKeysAndChildren.getKeyLocation(5));
            assertEquals(16, rightKeysAndChildren.getRightChild(5));

            assertEquals(16, rightKeysAndChildren.getLeftChild(6));
            assertEquals(new KeyLocation(6, (short) 6), rightKeysAndChildren.getKeyLocation(6));
            assertEquals(17, rightKeysAndChildren.getRightChild(6));

            assertEquals(7, rightKeysAndChildren.getNextKeyNum());
            assertEquals(0, leftKeysAndChildren.getNextKeyNum());
        }

        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(2, (short) 2)},
                    new long[]{10, 11, 12, 13});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(5, (short) 5),
                            new KeyLocation(6, (short) 6)},
                    new long[]{14, 15, 16});

            rightKeysAndChildren.insertKeyToHead(new KeyLocation(3, (short) 3));
            assertFalse(leftKeysAndChildren.isFull());

            rightKeysAndChildren.joinWithLeftNode(leftKeysAndChildren);

            assertFalse(rightKeysAndChildren.isFull());
            assertEquals(10, rightKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), rightKeysAndChildren.getKeyLocation(0));
            assertEquals(11, rightKeysAndChildren.getRightChild(0));

            assertEquals(11, rightKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), rightKeysAndChildren.getKeyLocation(1));
            assertEquals(12, rightKeysAndChildren.getRightChild(1));

            assertEquals(12, rightKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(2, (short) 2), rightKeysAndChildren.getKeyLocation(2));
            assertEquals(13, rightKeysAndChildren.getRightChild(2));

            assertEquals(13, rightKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(3, (short) 3), rightKeysAndChildren.getKeyLocation(3));
            assertEquals(14, rightKeysAndChildren.getRightChild(3));

            assertEquals(14, rightKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(5, (short) 5), rightKeysAndChildren.getKeyLocation(4));
            assertEquals(15, rightKeysAndChildren.getRightChild(4));

            assertEquals(15, rightKeysAndChildren.getLeftChild(5));
            assertEquals(new KeyLocation(6, (short) 6), rightKeysAndChildren.getKeyLocation(5));
            assertEquals(16, rightKeysAndChildren.getRightChild(5));

            assertEquals(0, leftKeysAndChildren.getNextKeyNum());
        }

        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(2, (short) 2)},
                    new long[]{10, 11, 12, 13});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5)},
                    new long[]{14, 15, 16});

            rightKeysAndChildren.insertKeyToHead(new KeyLocation(3, (short) 3));

            rightKeysAndChildren.joinWithLeftNode(leftKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(10, rightKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), rightKeysAndChildren.getKeyLocation(0));
            assertEquals(11, rightKeysAndChildren.getRightChild(0));

            assertEquals(11, rightKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), rightKeysAndChildren.getKeyLocation(1));
            assertEquals(12, rightKeysAndChildren.getRightChild(1));

            assertEquals(12, rightKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(2, (short) 2), rightKeysAndChildren.getKeyLocation(2));
            assertEquals(13, rightKeysAndChildren.getRightChild(2));

            assertEquals(13, rightKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(3, (short) 3), rightKeysAndChildren.getKeyLocation(3));
            assertEquals(14, rightKeysAndChildren.getRightChild(3));

            assertEquals(14, rightKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(4, (short) 4), rightKeysAndChildren.getKeyLocation(4));
            assertEquals(15, rightKeysAndChildren.getRightChild(4));

            assertEquals(15, rightKeysAndChildren.getLeftChild(5));
            assertEquals(new KeyLocation(5, (short) 5), rightKeysAndChildren.getKeyLocation(5));
            assertEquals(16, rightKeysAndChildren.getRightChild(5));

            assertEquals(0, leftKeysAndChildren.getNextKeyNum());
        }

        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys leftKeysAndChildren = create(btreeParams);
            leftKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1)},
                    new long[]{10, 11, 12});

            KeyNodeKeys rightKeysAndChildren = create(btreeParams);
            rightKeysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(4, (short) 4),
                            new KeyLocation(5, (short) 5)},
                    new long[]{14, 15, 16});

            rightKeysAndChildren.insertKeyToHead(new KeyLocation(3, (short) 3));

            rightKeysAndChildren.joinWithLeftNode(leftKeysAndChildren);

            assertFalse(leftKeysAndChildren.isFull());
            assertEquals(10, rightKeysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), rightKeysAndChildren.getKeyLocation(0));
            assertEquals(11, rightKeysAndChildren.getRightChild(0));

            assertEquals(11, rightKeysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), rightKeysAndChildren.getKeyLocation(1));
            assertEquals(12, rightKeysAndChildren.getRightChild(1));

            assertEquals(12, rightKeysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(3, (short) 3), rightKeysAndChildren.getKeyLocation(2));
            assertEquals(14, rightKeysAndChildren.getRightChild(2));

            assertEquals(14, rightKeysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(4, (short) 4), rightKeysAndChildren.getKeyLocation(3));
            assertEquals(15, rightKeysAndChildren.getRightChild(3));

            assertEquals(15, rightKeysAndChildren.getLeftChild(4));
            assertEquals(new KeyLocation(5, (short) 5), rightKeysAndChildren.getKeyLocation(4));
            assertEquals(16, rightKeysAndChildren.getRightChild(4));

            assertEquals(0, leftKeysAndChildren.getNextKeyNum());
        }
    }

    // addKeyToEnd
    @Test
    public void test19() {
        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys keysAndChildren = create(btreeParams);
            keysAndChildren.bulkInsertInEmptyNode(
                    new KeyLocation[]{
                            new KeyLocation(0, (short) 0),
                            new KeyLocation(1, (short) 1),
                            new KeyLocation(2, (short) 2)},
                    new long[]{10, 11, 12, 13});

            keysAndChildren.addKeyToEnd(new KeyLocation(3, (short) 3));

            assertEquals(10, keysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(0, (short) 0), keysAndChildren.getKeyLocation(0));
            assertEquals(11, keysAndChildren.getRightChild(0));

            assertEquals(11, keysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(1, (short) 1), keysAndChildren.getKeyLocation(1));
            assertEquals(12, keysAndChildren.getRightChild(1));

            assertEquals(12, keysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(2, (short) 2), keysAndChildren.getKeyLocation(2));
            assertEquals(13, keysAndChildren.getRightChild(2));

            assertEquals(13, keysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(3));
            assertEquals(0, keysAndChildren.getRightChild(3));
        }
    }

    // appendKeyAndLeftChild and addRightChildToEnd
    @Test
    public void test20() {
        {
            BtreeConf btreeParams = new BtreeConf(7);
            KeyNodeKeys keysAndChildren = create(btreeParams);
//            keysAndChildren.bulkInsertInEmptyNode(
//                    new KeyLocation[]{
//                            new KeyLocation(0, (short) 0),
//                            new KeyLocation(1, (short) 1),
//                            new KeyLocation(2, (short) 2)},
//                    new long[]{10, 11, 12, 13});

            keysAndChildren.appendKeyAndLeftChild(new KeyLocation(1, (short) 1), 1);
            keysAndChildren.appendKeyAndLeftChild(new KeyLocation(2, (short) 2), 2);
            keysAndChildren.appendKeyAndLeftChild(new KeyLocation(3, (short) 3), 3);
            keysAndChildren.appendKeyAndLeftChild(new KeyLocation(4, (short) 4), 4);
            keysAndChildren.addRightChildToEnd(5);

            assertEquals(1, keysAndChildren.getLeftChild(0));
            assertEquals(new KeyLocation(1, (short) 1), keysAndChildren.getKeyLocation(0));
            assertEquals(2, keysAndChildren.getRightChild(0));

            assertEquals(2, keysAndChildren.getLeftChild(1));
            assertEquals(new KeyLocation(2, (short) 2), keysAndChildren.getKeyLocation(1));
            assertEquals(3, keysAndChildren.getRightChild(1));

            assertEquals(3, keysAndChildren.getLeftChild(2));
            assertEquals(new KeyLocation(3, (short) 3), keysAndChildren.getKeyLocation(2));
            assertEquals(4, keysAndChildren.getRightChild(2));

            assertEquals(4, keysAndChildren.getLeftChild(3));
            assertEquals(new KeyLocation(4, (short) 4), keysAndChildren.getKeyLocation(3));
            assertEquals(5, keysAndChildren.getRightChild(3));

        }
    }



    private static KeyNodeKeys create(BtreeConf btreeConf) {
        ByteBuffer nextKeyBB = ByteBuffer.allocate(4);
        nextKeyBB.mark();
        nextKeyBB.putInt(0);

        byte[] bytes = new byte[1000];
        ByteBuffer keysAndChildrenBB = ByteBuffer.wrap(bytes, 88, btreeConf.leafNodeKVArrSize * (KeyLocation.SIZE + TypeSize.LONG) + TypeSize.LONG);
        keysAndChildrenBB.mark();

        return new KeyNodeKeys(nextKeyBB, keysAndChildrenBB, btreeConf);
    }





//    // extractRightHalf
//    @Test
//    public void test3345() {
//        BtreeParameters btreeParameters = new BtreeParameters(6);
//        NodeKeysAndChildren nodeKeysAndChildren = create(btreeParameters);
//        nodeKeysAndChildren.bulkInsertInEmptyNode(
//                new Location[]{
//                        new Location(0, (short) 0),
//                        new Location(1, (short) 1),
//                        new Location(2, (short) 2),
//                        new Location(3, (short) 3),
//                        new Location(4, (short) 4),
//                        new Location(5, (short) 5)},
//                new long[]{3, 4, 5, 6, 7, 8, 9});
//
//        assertEquals(new Location(0, (short) 0), nodeKeysAndChildren.getIndexLocation(0));
//        assertEquals(new Location(1, (short) 1), nodeKeysAndChildren.getIndexLocation(1));
//        assertEquals(new Location(2, (short) 2), nodeKeysAndChildren.getIndexLocation(2));
//        assertEquals(new Location(3, (short) 3), nodeKeysAndChildren.getIndexLocation(3));
//        assertEquals(new Location(4, (short) 4), nodeKeysAndChildren.getIndexLocation(4));
//        assertEquals(new Location(5, (short) 5), nodeKeysAndChildren.getIndexLocation(5));
//        assertTrue(nodeKeysAndChildren.isFull());
//        assertEquals(new Location(3, (short) 3), nodeKeysAndChildren.getMidIndex());
//
//        ByteBuffer rightHalfBB = nodeKeysAndChildren.extractRightHalf();
//        assertEquals(7, rightHalfBB.getLong());
//        assertEquals(4, rightHalfBB.getLong());
//        assertEquals(4, rightHalfBB.getShort());
//
//        assertEquals(8, rightHalfBB.getLong());
//        assertEquals(5, rightHalfBB.getLong());
//        assertEquals(5, rightHalfBB.getShort());
//
//        assertEquals(9, rightHalfBB.getLong());
//
//        assertEquals(3, nodeKeysAndChildren.getNextKeyNum());
//    }
//
//    // extractRightHalf
//    @Test
//    public void test4455() {
//        BtreeParameters btreeParameters = new BtreeParameters(5);
//        NodeKeysAndChildren nodeKeysAndChildren = create(btreeParameters);
//        nodeKeysAndChildren.bulkInsertInEmptyNode(
//                new Location[]{
//                        new Location(0, (short) 0),
//                        new Location(1, (short) 1),
//                        new Location(2, (short) 2),
//                        new Location(3, (short) 3),
//                        new Location(4, (short) 4)},
//                new long[]{3, 4, 5, 6, 7, 8});
//
//        assertEquals(new Location(0, (short) 0), nodeKeysAndChildren.getIndexLocation(0));
//        assertEquals(new Location(1, (short) 1), nodeKeysAndChildren.getIndexLocation(1));
//        assertEquals(new Location(2, (short) 2), nodeKeysAndChildren.getIndexLocation(2));
//        assertEquals(new Location(3, (short) 3), nodeKeysAndChildren.getIndexLocation(3));
//        assertEquals(new Location(4, (short) 4), nodeKeysAndChildren.getIndexLocation(4));
//        assertTrue(nodeKeysAndChildren.isFull());
//        assertEquals(new Location(2, (short) 2), nodeKeysAndChildren.getMidIndex());
//
//        ByteBuffer rightHalfBB = nodeKeysAndChildren.extractRightHalf();
//        assertEquals(6, rightHalfBB.getLong());
//        assertEquals(3, rightHalfBB.getLong());
//        assertEquals(3, rightHalfBB.getShort());
//
//        assertEquals(7, rightHalfBB.getLong());
//        assertEquals(4, rightHalfBB.getLong());
//        assertEquals(4, rightHalfBB.getShort());
//
//        assertEquals(8, rightHalfBB.getLong());
//
//        assertEquals(2, nodeKeysAndChildren.getNextKeyNum());
//    }

}