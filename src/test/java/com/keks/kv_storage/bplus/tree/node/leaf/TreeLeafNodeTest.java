package com.keks.kv_storage.bplus.tree.node.leaf;

import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.TableName;
import com.keks.kv_storage.bplus.page_manager.BplusTreeRuntimeParameters;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.page_manager.managers.TreeLeafNodePageManager;
import com.keks.kv_storage.bplus.tree.KeyToDataLocationsItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;


class TreeLeafNodeTest {

    private static final TableName tableName = new TableName("test");

    // assert  manual insert
    @Test
    public void test1(@TempDir Path dir) throws IOException {
        int btreeOrder = 3;
        PageBuffer myBuffer = new PageBuffer(10);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        BPlusConf bPlusConf = new BPlusConf(btreeOrder, 10, 10,40_000_00);
        BplusTreeRuntimeParameters bplusTreeRuntimeParameters = new BplusTreeRuntimeParameters(btreeOrder, tableDir);
        TreeLeafNodePageManager treeLeafNodePageManager = new TreeLeafNodePageManager(bplusTreeRuntimeParameters, bPlusConf, tableDir, myBuffer);
        IndexPageManager indexPageManager = new IndexPageManager(tableDir, myBuffer, bPlusConf);

        CachedPageNew<TreeLeafNode> indexPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(1L);
        TreeLeafNode leafNode = indexPage.getPage();

        KeyToDataLocationsItem keyToDataLocationsB = new KeyToDataLocationsItem("b", 10, new LeafDataLocation[]{
                new LeafDataLocation(123, (short) 1),
                new LeafDataLocation(124, (short) 2),
                new LeafDataLocation(125, (short) 3),
        });
        KeyToDataLocationsItem keyToDataLocationsA = new KeyToDataLocationsItem("a", 10, new LeafDataLocation[]{
                new LeafDataLocation(123, (short) 1),
                new LeafDataLocation(124, (short) 2),
                new LeafDataLocation(125, (short) 3),
        });
        KeyToDataLocationsItem keyToDataLocationsC = new KeyToDataLocationsItem("c", 10, new LeafDataLocation[]{
                new LeafDataLocation(123, (short) 1),
                new LeafDataLocation(124, (short) 2),
                new LeafDataLocation(125, (short) 3),
        });

//        System.out.println(leafNode);
        {

            int insertPos = leafNode.getInsertPos(keyToDataLocationsB.key, indexPageManager);
            LeafDataLocation indexLocation = indexPageManager.addLeafDataLocations(keyToDataLocationsB);
            leafNode.leafKeys.insertNewKey((insertPos + 1) * -1, indexLocation);

            assertEquals(indexLocation, leafNode.getKeyLocation(keyToDataLocationsB.key, indexPageManager));
        }
//        System.out.println(leafNode);
        {

            int insertPos = leafNode.getInsertPos(keyToDataLocationsA.key, indexPageManager);
            LeafDataLocation indexLocation = indexPageManager.addLeafDataLocations(keyToDataLocationsA);
            leafNode.leafKeys.insertNewKey((insertPos + 1) * -1, indexLocation);
            assertEquals(indexLocation, leafNode.getKeyLocation(keyToDataLocationsA.key, indexPageManager));
        }
//        System.out.println(leafNode);
        {

            int insertPos = leafNode.getInsertPos(keyToDataLocationsC.key, indexPageManager);
            LeafDataLocation indexLocation = indexPageManager.addLeafDataLocations(keyToDataLocationsC);
            leafNode.leafKeys.insertNewKey((insertPos + 1) * -1, indexLocation);
            assertEquals(indexLocation, leafNode.getKeyLocation(keyToDataLocationsC.key, indexPageManager));
        }
//        System.out.println(leafNode);

    }

    // insert by for cycle
    @Test
    public void test2(@TempDir Path dir) throws IOException {
        int btreeOrder = 10;
        PageBuffer myBuffer = new PageBuffer(10);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        BPlusConf bPlusConf = new BPlusConf(10, 10, 10,40_000_00);
        BplusTreeRuntimeParameters bplusTreeRuntimeParameters = new BplusTreeRuntimeParameters(btreeOrder, tableDir);
        TreeLeafNodePageManager treeLeafNodePageManager = new TreeLeafNodePageManager(bplusTreeRuntimeParameters, bPlusConf, tableDir, myBuffer);
        IndexPageManager indexPageManager = new IndexPageManager(tableDir, myBuffer, bPlusConf);

        CachedPageNew<TreeLeafNode> indexPage = treeLeafNodePageManager.getAndLockReadLeafNodePage(1L);
        TreeLeafNode leafNode = indexPage.page;

        ConcurrentHashMap<Integer, LeafDataLocation> locationMap = new ConcurrentHashMap<>();

        for (int i = 1; i <= bPlusConf.btreeConf.leafNodeKVArrSize; i++) {
            KeyToDataLocationsItem keyToDataLocations = new KeyToDataLocationsItem(i + "", 10, new LeafDataLocation[]{
                    new LeafDataLocation(i + 5, (short) 1),
                    new LeafDataLocation(i + 10, (short) 2),
                    new LeafDataLocation(i + 15, (short) 3),
            });
            int insertPos = leafNode.getInsertPos(keyToDataLocations.key, indexPageManager);
            LeafDataLocation indexLocation = indexPageManager.addLeafDataLocations(keyToDataLocations);
            leafNode.leafKeys.insertNewKey((insertPos + 1) * -1, indexLocation);
            locationMap.put(i, indexLocation);
        }

        for (int i = 1; i <= bPlusConf.btreeConf.leafNodeKVArrSize; i++) {
            KeyToDataLocationsItem keyToDataLocations = new KeyToDataLocationsItem(i + "", 10, new LeafDataLocation[]{
                    new LeafDataLocation(i + 5, (short) 1),
                    new LeafDataLocation(i + 10, (short) 2),
                    new LeafDataLocation(i + 15, (short) 3),
            });
            assertEquals(locationMap.get(i), leafNode.getKeyLocation(keyToDataLocations.key, indexPageManager));
        }
    }

    // insert and update by for cycle
    @Test
    public void test3(@TempDir Path dir) throws IOException {
        int btreeOrder = 10;
        PageBuffer myBuffer = new PageBuffer(10);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        BPlusConf bPlusConf = new BPlusConf(btreeOrder, 10, 10,40_000_00);
        BplusTreeRuntimeParameters bplusTreeRuntimeParameters = new BplusTreeRuntimeParameters(btreeOrder, tableDir);
        TreeLeafNodePageManager treeLeafNodePageManager = new TreeLeafNodePageManager(bplusTreeRuntimeParameters, bPlusConf, tableDir, myBuffer);
        IndexPageManager indexPageManager = new IndexPageManager(tableDir, myBuffer, bPlusConf);

        CachedPageNew<TreeLeafNode> indexPage = treeLeafNodePageManager.getAndLockReadLeafNodePage(1L);


        TreeLeafNode leafNode = indexPage.page;

        ConcurrentHashMap<Integer, LeafDataLocation> locationMap = new ConcurrentHashMap<>();

        for (int i = 1; i <= bPlusConf.btreeConf.leafNodeKVArrSize; i++) {
            KeyToDataLocationsItem keyToDataLocations = new KeyToDataLocationsItem(i + "", 10, new LeafDataLocation[]{
                    new LeafDataLocation(i + 5, (short) 1),
                    new LeafDataLocation(i + 10, (short) 2),
                    new LeafDataLocation(i + 15, (short) 3),
            });
            int insertPos = leafNode.getInsertPos(keyToDataLocations.key, indexPageManager);
            LeafDataLocation indexLocation = indexPageManager.addLeafDataLocations(keyToDataLocations);
            leafNode.leafKeys.insertNewKey((insertPos + 1) * -1, indexLocation);
            locationMap.put(i, indexLocation);
        }

        for (int i = 1; i <= bPlusConf.btreeConf.leafNodeKVArrSize; i++) {
            KeyToDataLocationsItem keyToDataLocations = new KeyToDataLocationsItem(i + "", 10, new LeafDataLocation[]{
                    new LeafDataLocation(i + 5, (short) 1),
                    new LeafDataLocation(i + 10, (short) 2),
                    new LeafDataLocation(i + 15, (short) 3),
            });
            assertEquals(locationMap.get(i), leafNode.getKeyLocation(keyToDataLocations.key, indexPageManager));
        }

        for (int i = 1; i <= bPlusConf.btreeConf.leafNodeKVArrSize; i++) {
            if (i % 2 == 0) {
                KeyToDataLocationsItem previousKeyToDataLocations = new KeyToDataLocationsItem(i + "", 10, new LeafDataLocation[]{
                        new LeafDataLocation(i + 5, (short) 1),
                        new LeafDataLocation(i + 10, (short) 2),
                        new LeafDataLocation(i + 15, (short) 3),
                });
                KeyToDataLocationsItem newKeyToDataLocations = new KeyToDataLocationsItem(i + "", 20, new LeafDataLocation[]{
                        new LeafDataLocation(i + 10, (short) 1),
                        new LeafDataLocation(i + 15, (short) 2),
                        new LeafDataLocation(i + 20, (short) 3),
                        new LeafDataLocation(i + 25, (short) 4),
                });
                int keyNum = leafNode.getInsertPos(previousKeyToDataLocations.key, indexPageManager);
                LeafDataLocation previousIndexLocation = leafNode.leafKeys.getKeyLocation(keyNum);
                LeafDataLocation newIndexLocation = indexPageManager.updateKeyToDataLocations(previousIndexLocation, previousKeyToDataLocations.getLen(), newKeyToDataLocations);
                leafNode.leafKeys.replaceWithNewKey(keyNum, newIndexLocation);
                locationMap.put(i, newIndexLocation);
            }
        }

        for (int i = 1; i <= bPlusConf.btreeConf.leafNodeKVArrSize; i++) {
            KeyToDataLocationsItem keyToDataLocations;
            if (i % 2 == 0) {
                keyToDataLocations = new KeyToDataLocationsItem(i + "", 10, new LeafDataLocation[]{
                        new LeafDataLocation(i + 10, (short) 1),
                        new LeafDataLocation(i + 15, (short) 2),
                        new LeafDataLocation(i + 20, (short) 3),
                });
            } else {
                keyToDataLocations = new KeyToDataLocationsItem(i + "", 10, new LeafDataLocation[]{
                        new LeafDataLocation(i + 5, (short) 1),
                        new LeafDataLocation(i + 10, (short) 2),
                        new LeafDataLocation(i + 15, (short) 3),
                });
            }
            assertEquals(locationMap.get(i), leafNode.getKeyLocation(keyToDataLocations.key, indexPageManager));

        }

    }

}