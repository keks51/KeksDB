package com.keks.kv_storage.bplus.tree.node.leaf;

import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.conf.BtreeConf;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;


class LeafNodeKeysTest {

    // insert
    @Test
    public void test1() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        assertEquals(0, leafKeys.getNextKeyNum());
        // 1 2 0 4 3
        leafKeys.insertNewKey(0, new LeafDataLocation(1, (short) 1));
        leafKeys.insertNewKey(1, new LeafDataLocation(2, (short) 2));
        leafKeys.insertNewKey(2, new LeafDataLocation(0, (short) 0));
        leafKeys.insertNewKey(0, new LeafDataLocation(4, (short) 4));
        leafKeys.insertNewKey(2, new LeafDataLocation(3, (short) 3));

        assertEquals(5, leafKeys.getNextKeyNum());
        assertEquals(new LeafDataLocation(4, (short) 4), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(2));
        assertEquals(new LeafDataLocation(2, (short) 2), leafKeys.getKeyLocation(3));
        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(4));
        assertEquals(5, leafKeys.getNextKeyNum());
    }

    // delete all
    @Test
    public void test2() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        // 1 2 0 4 3
        leafKeys.insertNewKey(0, new LeafDataLocation(1, (short) 1)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(2, (short) 2)); // 1
        leafKeys.insertNewKey(2, new LeafDataLocation(0, (short) 0)); // 2
        leafKeys.insertNewKey(0, new LeafDataLocation(4, (short) 4)); // 3
        leafKeys.insertNewKey(2, new LeafDataLocation(3, (short) 3)); // 4

        leafKeys.delete(4);
        leafKeys.delete(2);
        leafKeys.delete(1);
        leafKeys.delete(0);
        leafKeys.delete(0);
        assertEquals(0, leafKeys.getNextKeyNum());
    }

    // insert and delete
    @Test
    public void test3() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        // 4 1 3 2 0
        leafKeys.insertNewKey(0, new LeafDataLocation(1, (short) 1)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(2, (short) 2)); // 1
        leafKeys.insertNewKey(2, new LeafDataLocation(0, (short) 0)); // 2
        leafKeys.insertNewKey(0, new LeafDataLocation(4, (short) 4)); // 3
        leafKeys.insertNewKey(2, new LeafDataLocation(3, (short) 3)); // 4
        System.out.println(leafKeys.toString1());

        leafKeys.delete(3);
        leafKeys.delete(3);
        assertEquals(new LeafDataLocation(4, (short) 4), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(2));
        assertEquals(3, leafKeys.getNextKeyNum());

        leafKeys.delete(1);
        assertEquals(new LeafDataLocation(4, (short) 4), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(1));
        assertEquals(2, leafKeys.getNextKeyNum());

        leafKeys.delete(0);
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(0));
        assertEquals(1, leafKeys.getNextKeyNum());

        leafKeys.delete(0);
        assertEquals(0, leafKeys.getNextKeyNum());

        // 0 3 1 4 2
        leafKeys.insertNewKey(0, new LeafDataLocation(1, (short) 1));
        leafKeys.insertNewKey(1, new LeafDataLocation(2, (short) 2));
        leafKeys.insertNewKey(0, new LeafDataLocation(0, (short) 0));
        leafKeys.insertNewKey(2, new LeafDataLocation(4, (short) 4));
        leafKeys.insertNewKey(1, new LeafDataLocation(3, (short) 3));

        assertEquals(5, leafKeys.getNextKeyNum());
        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(2));
        assertEquals(new LeafDataLocation(4, (short) 4), leafKeys.getKeyLocation(3));
        assertEquals(new LeafDataLocation(2, (short) 2), leafKeys.getKeyLocation(4));
        assertEquals(5, leafKeys.getNextKeyNum());

        leafKeys.delete(4);
        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(2));
        assertEquals(new LeafDataLocation(4, (short) 4), leafKeys.getKeyLocation(3));
        assertEquals(4, leafKeys.getNextKeyNum());

        leafKeys.delete(3);
        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(2));
        assertEquals(3, leafKeys.getNextKeyNum());

        leafKeys.insertNewKey(3, new LeafDataLocation(4, (short) 4));
        leafKeys.insertNewKey(4, new LeafDataLocation(5, (short) 5));

        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(3, (short) 3), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(2));
        assertEquals(new LeafDataLocation(4, (short) 4), leafKeys.getKeyLocation(3));
        assertEquals(new LeafDataLocation(5, (short) 5), leafKeys.getKeyLocation(4));
        assertEquals(5, leafKeys.getNextKeyNum());
    }

    // replace
    @Test
    public void test4() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        assertEquals(0, leafKeys.getNextKeyNum());
        // 1 2 0 4 3
        leafKeys.insertNewKey(0, new LeafDataLocation(1, (short) 1));
        leafKeys.insertNewKey(1, new LeafDataLocation(2, (short) 2));
        leafKeys.insertNewKey(2, new LeafDataLocation(5, (short) 5));
        leafKeys.insertNewKey(0, new LeafDataLocation(4, (short) 4));
        leafKeys.insertNewKey(2, new LeafDataLocation(3, (short) 3));

        leafKeys.replaceWithNewKey(0, new LeafDataLocation(44, (short) 44));
        leafKeys.replaceWithNewKey(1, new LeafDataLocation(11, (short) 11));
        leafKeys.replaceWithNewKey(2, new LeafDataLocation(33, (short) 33));
        leafKeys.replaceWithNewKey(3, new LeafDataLocation(22, (short) 22));
        leafKeys.replaceWithNewKey(4, new LeafDataLocation(55, (short) 55));

        assertEquals(new LeafDataLocation(44, (short) 44), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(11, (short) 11), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(33, (short) 33), leafKeys.getKeyLocation(2));
        assertEquals(new LeafDataLocation(22, (short) 22), leafKeys.getKeyLocation(3));
        assertEquals(new LeafDataLocation(55, (short) 55), leafKeys.getKeyLocation(4));
        assertEquals(5, leafKeys.getNextKeyNum());
    }

    // getMidIndex
    @Test
    public void test5() {
        {
            LeafNodeKeys leafKeys = create(new BtreeConf(3),
                    new LeafDataLocation[] {
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });
            assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getMidKeyDataLocation());
        }
        {
            LeafNodeKeys leafKeys = create(new BtreeConf(4),
                    new LeafDataLocation[] {
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2),
                            new LeafDataLocation(3, (short) 3)
                    });
            assertEquals(new LeafDataLocation(2, (short) 2), leafKeys.getMidKeyDataLocation());
        }
        {
            LeafNodeKeys leafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[] {
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2),
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4),
                    });
            assertEquals(new LeafDataLocation(2, (short) 2), leafKeys.getMidKeyDataLocation());
        }
    }

    // moveRightHalfToNewRightNode
    @Test
    public void test6() {
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(3),
                    new LeafDataLocation[] {
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(3));
            leftLeafKeys.moveRightHalfToNewRightNode(rightLeafKeys);

            assertEquals(new LeafDataLocation(0, (short) 0), leftLeafKeys.getKeyLocation(0));
            assertEquals(1, leftLeafKeys.getNextKeyNum());

            assertEquals(new LeafDataLocation(1, (short) 1), rightLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(2, (short) 2), rightLeafKeys.getKeyLocation(1));
            assertEquals(2, rightLeafKeys.getNextKeyNum());


        }
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(4),
                    new LeafDataLocation[] {
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2),
                            new LeafDataLocation(3, (short) 3)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(4));
            leftLeafKeys.moveRightHalfToNewRightNode(rightLeafKeys);

            assertEquals(new LeafDataLocation(0, (short) 0), leftLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), leftLeafKeys.getKeyLocation(1));
            assertEquals(2, leftLeafKeys.getNextKeyNum());

            assertEquals(new LeafDataLocation(2, (short) 2), rightLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(3, (short) 3), rightLeafKeys.getKeyLocation(1));
            assertEquals(2, rightLeafKeys.getNextKeyNum());
        }
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[] {
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2),
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4),
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(5));
            leftLeafKeys.moveRightHalfToNewRightNode(rightLeafKeys);

            assertEquals(new LeafDataLocation(0, (short) 0), leftLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), leftLeafKeys.getKeyLocation(1));
            assertEquals(2, leftLeafKeys.getNextKeyNum());

            assertEquals(new LeafDataLocation(2, (short) 2), rightLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(3, (short) 3), rightLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(4, (short) 4), rightLeafKeys.getKeyLocation(2));
            assertEquals(3, rightLeafKeys.getNextKeyNum());

        }
    }

    // addRightNode
    @Test
    public void test7() {
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4),
                            new LeafDataLocation(5, (short) 5)
                    });
            assertFalse(leftLeafKeys.isFull());
            leftLeafKeys.addRightNode(rightLeafKeys);

            assertTrue(leftLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), leftLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), leftLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(2, (short) 2), leftLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(3, (short) 3), leftLeafKeys.getKeyLocation(3));
            assertEquals(new LeafDataLocation(4, (short) 4), leftLeafKeys.getKeyLocation(4));
            assertEquals(new LeafDataLocation(5, (short) 5), leftLeafKeys.getKeyLocation(5));
            assertEquals(6, leftLeafKeys.getNextKeyNum());
            assertEquals(0, rightLeafKeys.getNextKeyNum());
        }

        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4),
                            new LeafDataLocation(5, (short) 5)
                    });
            leftLeafKeys.addRightNode(rightLeafKeys);

            assertTrue(leftLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), leftLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), leftLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(3, (short) 3), leftLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(4, (short) 4), leftLeafKeys.getKeyLocation(3));
            assertEquals(new LeafDataLocation(5, (short) 5), leftLeafKeys.getKeyLocation(4));
            assertEquals(5, leftLeafKeys.getNextKeyNum());
            assertEquals(0, rightLeafKeys.getNextKeyNum());
        }
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(4, (short) 4),
                            new LeafDataLocation(5, (short) 5)
                    });
            leftLeafKeys.addRightNode(rightLeafKeys);

            assertTrue(leftLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), leftLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), leftLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(2, (short) 2), leftLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(4, (short) 4), leftLeafKeys.getKeyLocation(3));
            assertEquals(new LeafDataLocation(5, (short) 5), leftLeafKeys.getKeyLocation(4));
            assertEquals(5, leftLeafKeys.getNextKeyNum());
            assertEquals(0, rightLeafKeys.getNextKeyNum());
        }
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4)
                    });
            leftLeafKeys.addRightNode(rightLeafKeys);

            assertFalse(leftLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), leftLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), leftLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(3, (short) 3), leftLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(4, (short) 4), leftLeafKeys.getKeyLocation(3));
            assertEquals(4, leftLeafKeys.getNextKeyNum());
            assertEquals(0, rightLeafKeys.getNextKeyNum());
        }
    }

    @Test
    public void test8() {
        // tree order = 3
        {
            BtreeConf btreeParams = new BtreeConf(3);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(3, (short) 3)});

            assertFalse(leafKeys.isDeficient());
            assertFalse(leafKeys.isMergeable());
            assertTrue(leafKeys.isLendable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(3);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0)});

            assertFalse(leafKeys.isDeficient());
            assertFalse(leafKeys.isLendable());
            assertTrue(leafKeys.isMergeable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(3);
            LeafNodeKeys leafKeys = create(btreeParams);

            assertTrue(leafKeys.isDeficient());
            assertFalse(leafKeys.isLendable());
            assertFalse(leafKeys.isMergeable());

            assertTrue(leafKeys.isEmpty());
        }


        // tree order = 4
        {
            BtreeConf btreeParams = new BtreeConf(4);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4)});

            assertFalse(leafKeys.isDeficient());
            assertFalse(leafKeys.isMergeable());
            assertTrue(leafKeys.isLendable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(4);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(3, (short) 3)});

            assertFalse(leafKeys.isDeficient());
            assertFalse(leafKeys.isMergeable());
            assertTrue(leafKeys.isLendable());

        }

        {
            BtreeConf btreeParams = new BtreeConf(4);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0)});

            assertFalse(leafKeys.isDeficient());
            assertTrue(leafKeys.isMergeable());
            assertFalse(leafKeys.isLendable());

        }

        {
            BtreeConf btreeParams = new BtreeConf(4);
            LeafNodeKeys leafKeys = create(btreeParams);

            assertTrue(leafKeys.isDeficient());
            assertFalse(leafKeys.isLendable());
            assertFalse(leafKeys.isMergeable());

            assertTrue(leafKeys.isEmpty());
        }


        // tree order = 5
        {
            BtreeConf btreeParams = new BtreeConf(5);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4)});

            assertFalse(leafKeys.isDeficient());
            assertFalse(leafKeys.isMergeable());
            assertTrue(leafKeys.isLendable());
        }

        {
            BtreeConf btreeParams = new BtreeConf(5);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(3, (short) 3)});

            assertFalse(leafKeys.isDeficient());
            assertTrue(leafKeys.isMergeable());
            assertFalse(leafKeys.isLendable());

        }

        {
            BtreeConf btreeParams = new BtreeConf(5);
            LeafNodeKeys leafKeys = create(btreeParams,
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0)});

            assertTrue(leafKeys.isDeficient());
            assertFalse(leafKeys.isMergeable());
            assertFalse(leafKeys.isLendable());

        }

    }

    // addLeftNode
    @Test
    public void test9() {
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4),
                            new LeafDataLocation(5, (short) 5)
                    });
            assertFalse(leftLeafKeys.isFull());
            rightLeafKeys.addLeftNode(leftLeafKeys);

            assertTrue(rightLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), rightLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), rightLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(2, (short) 2), rightLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(3, (short) 3), rightLeafKeys.getKeyLocation(3));
            assertEquals(new LeafDataLocation(4, (short) 4), rightLeafKeys.getKeyLocation(4));
            assertEquals(new LeafDataLocation(5, (short) 5), rightLeafKeys.getKeyLocation(5));
            assertEquals(6, rightLeafKeys.getNextKeyNum());
            assertEquals(0, leftLeafKeys.getNextKeyNum());
        }

        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4),
                            new LeafDataLocation(5, (short) 5)
                    });
            rightLeafKeys.addLeftNode(leftLeafKeys);

            assertTrue(rightLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), rightLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), rightLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(3, (short) 3), rightLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(4, (short) 4), rightLeafKeys.getKeyLocation(3));
            assertEquals(new LeafDataLocation(5, (short) 5), rightLeafKeys.getKeyLocation(4));
            assertEquals(5, rightLeafKeys.getNextKeyNum());
            assertEquals(0, leftLeafKeys.getNextKeyNum());
        }
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(5),
                    new LeafDataLocation[]{
                            new LeafDataLocation(4, (short) 4),
                            new LeafDataLocation(5, (short) 5)
                    });
            rightLeafKeys.addLeftNode(leftLeafKeys);

            assertTrue(rightLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), rightLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), rightLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(2, (short) 2), rightLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(4, (short) 4), rightLeafKeys.getKeyLocation(3));
            assertEquals(new LeafDataLocation(5, (short) 5), rightLeafKeys.getKeyLocation(4));
            assertEquals(5, rightLeafKeys.getNextKeyNum());
            assertEquals(0, leftLeafKeys.getNextKeyNum());
        }
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1)
                    });
            LeafNodeKeys rightLeafKeys = create(new BtreeConf(6),
                    new LeafDataLocation[]{
                            new LeafDataLocation(3, (short) 3),
                            new LeafDataLocation(4, (short) 4)
                    });
            rightLeafKeys.addLeftNode(leftLeafKeys);

            assertFalse(rightLeafKeys.isFull());
            assertEquals(new LeafDataLocation(0, (short) 0), rightLeafKeys.getKeyLocation(0));
            assertEquals(new LeafDataLocation(1, (short) 1), rightLeafKeys.getKeyLocation(1));
            assertEquals(new LeafDataLocation(3, (short) 3), rightLeafKeys.getKeyLocation(2));
            assertEquals(new LeafDataLocation(4, (short) 4), rightLeafKeys.getKeyLocation(3));
            assertEquals(4, rightLeafKeys.getNextKeyNum());
            assertEquals(0, leftLeafKeys.getNextKeyNum());
        }
    }

    // delete first
    @Test
    public void test10() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        // 1 2 0 4 3
        leafKeys.insertNewKey(0, new LeafDataLocation(0, (short) 0)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(1, (short) 1)); // 1
        leafKeys.insertNewKey(2, new LeafDataLocation(2, (short) 2)); // 2

        leafKeys.removeFirst();
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(2, (short) 2), leafKeys.getKeyLocation(1));

        assertEquals(2, leafKeys.getNextKeyNum());
    }

    // delete last
    @Test
    public void test11() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        leafKeys.insertNewKey(0, new LeafDataLocation(0, (short) 0)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(1, (short) 1)); // 1
        leafKeys.insertNewKey(2, new LeafDataLocation(2, (short) 2)); // 2

        leafKeys.removeLast();
        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(1));

        assertEquals(2, leafKeys.getNextKeyNum());
    }

    // add head
    @Test
    public void test12() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        leafKeys.insertNewKey(0, new LeafDataLocation(0, (short) 0)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(1, (short) 1)); // 1

        leafKeys.addHead(new LeafDataLocation(2, (short) 2));
        assertEquals(new LeafDataLocation(2, (short) 2), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(2));

        assertEquals(3, leafKeys.getNextKeyNum());
    }

    // add tail
    @Test
    public void test13() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        leafKeys.insertNewKey(0, new LeafDataLocation(0, (short) 0)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(1, (short) 1)); // 1

        leafKeys.addTail(new LeafDataLocation(2, (short) 2));

        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getKeyLocation(0));
        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getKeyLocation(1));
        assertEquals(new LeafDataLocation(2, (short) 2), leafKeys.getKeyLocation(2));

        assertEquals(3, leafKeys.getNextKeyNum());
    }

    // get head
    @Test
    public void test14() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        leafKeys.insertNewKey(0, new LeafDataLocation(0, (short) 0)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(1, (short) 1)); // 1

        assertEquals(new LeafDataLocation(0, (short) 0), leafKeys.getHead());

        assertEquals(2, leafKeys.getNextKeyNum());
    }

    // get last
    @Test
    public void test15() {
        LeafNodeKeys leafKeys = create(new BtreeConf(5));

        leafKeys.insertNewKey(0, new LeafDataLocation(0, (short) 0)); // 0
        leafKeys.insertNewKey(1, new LeafDataLocation(1, (short) 1)); // 1

        assertEquals(new LeafDataLocation(1, (short) 1), leafKeys.getLast());

        assertEquals(2, leafKeys.getNextKeyNum());
    }

    // isAlmostFull
    @Test
    public void test16() {
        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(3),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });

            assertFalse(leftLeafKeys.isAlmostFull());

        }

        {
            LeafNodeKeys leftLeafKeys = create(new BtreeConf(4),
                    new LeafDataLocation[]{
                            new LeafDataLocation(0, (short) 0),
                            new LeafDataLocation(1, (short) 1),
                            new LeafDataLocation(2, (short) 2)
                    });

            assertTrue(leftLeafKeys.isAlmostFull());

        }
    }



    private static LeafNodeKeys create(BtreeConf btreeConf) {
        return create(btreeConf, new LeafDataLocation[]{});
    }

    private static LeafNodeKeys create(BtreeConf btreeConf, LeafDataLocation[] keys) {
        ByteBuffer nextKeyBB = ByteBuffer.allocate(4);
        nextKeyBB.mark();
        nextKeyBB.putInt(0);

        byte[] bytes = new byte[1000];
        ByteBuffer keysAndChildrenBB = ByteBuffer.wrap(bytes, 88, btreeConf.leafNodeKVArrSize * (LeafDataLocation.SIZE ));
        keysAndChildrenBB.mark();

        return new LeafNodeKeys(nextKeyBB, keysAndChildrenBB, btreeConf, keys);
    }

}