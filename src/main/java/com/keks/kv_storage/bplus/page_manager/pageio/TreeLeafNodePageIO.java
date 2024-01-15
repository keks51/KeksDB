package com.keks.kv_storage.bplus.page_manager.pageio;

import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.page_disk.fixed.TreeNodePage;
import com.keks.kv_storage.bplus.tree.node.leaf.TreeLeafNode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


public class TreeLeafNodePageIO extends PageIO<TreeLeafNode> {

    private final BtreeConf btreeConf;

    public TreeLeafNodePageIO(File file, String fileName, BtreeConf btreeConf) throws IOException {
        super(file, fileName, TreeNodePage.DEFAULT_PAGE_SIZE);
        this.btreeConf = btreeConf;
    }

    @Override
    public TreeLeafNode getPage(long pageId) throws IOException {
        ByteBuffer bb = readBB(pageId);
        if (bb.getShort() == 0) { // if page is doesn't exist)
            return new TreeLeafNode(pageId, btreeConf);
        } else {
            bb.position(0);
            return new TreeLeafNode(bb, btreeConf);
        }
    }

}
