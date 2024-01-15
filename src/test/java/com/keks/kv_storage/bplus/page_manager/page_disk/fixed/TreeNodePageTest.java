package com.keks.kv_storage.bplus.page_manager.page_disk.fixed;

import com.keks.kv_storage.TypeSize;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;


class TreeNodePageTest {

    @Test
    public void test1() {
        TreeNodePage treeNodePage = new TreeNodePage(1);
        int nextKey = 1;
        long parentPage = 2;
        long leftSiblingPage = 3;
        long rightSiblingPage = 4;
        assertFalse(treeNodePage.isFull());
        {
            treeNodePage.getNextKeyBuf().putInt(nextKey);
            assertTrue(treeNodePage.isFull());

            treeNodePage.getParentPageBuf().putLong(parentPage);

            treeNodePage.getLeftSiblingPageBuf().putLong(leftSiblingPage);

            treeNodePage.getRightSiblingPageBuf().putLong(rightSiblingPage);

            ByteBuffer keysBuf = treeNodePage.getKeysBuf();
            int arrBufSize = TreeNodePage.DEFAULT_PAGE_SIZE - TreeNodePage.KEYS_ARR_POS;
            assertEquals(arrBufSize, keysBuf.remaining());
            long i = 5;
            while (keysBuf.remaining() >= TypeSize.LONG) {
                keysBuf.putLong(i);
                i++;
            }
        }
        {
            assertEquals(nextKey, treeNodePage.getNextKeyBuf().getInt());
            assertEquals(parentPage, treeNodePage.getParentPageBuf().getLong());
            assertEquals(leftSiblingPage, treeNodePage.getLeftSiblingPageBuf().getLong());
            assertEquals(rightSiblingPage, treeNodePage.getRightSiblingPageBuf().getLong());

            ByteBuffer keysBuf = treeNodePage.getKeysBuf();
            long i = 5;
            while (keysBuf.remaining() >= TypeSize.LONG) {
                assertEquals(i, keysBuf.getLong());
                i++;
            }
        }

        treeNodePage.emptyPage();
        {
            assertFalse(treeNodePage.isFull());
            assertEquals(0, treeNodePage.getNextKeyBuf().getInt());
            assertEquals(0, treeNodePage.getParentPageBuf().getLong());
            assertEquals(0, treeNodePage.getLeftSiblingPageBuf().getLong());
            assertEquals(0, treeNodePage.getRightSiblingPageBuf().getLong());

            ByteBuffer keysBuf = treeNodePage.getKeysBuf();
            while (keysBuf.remaining() >= TypeSize.LONG) {
                assertEquals(0, keysBuf.getLong());
            }
        }


    }

}