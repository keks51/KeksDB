package com.keks.kv_storage.bplus.page_manager.pageio;

import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.page_disk.fixed.TreeNodePage;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


public class TreeKeyNodePageIO extends PageIO<TreeKeyNode> {

    private final BtreeConf btreeConf;

    public TreeKeyNodePageIO(File file,
                             String fileName,
                             BtreeConf btreeConf) throws IOException {
        super(file, fileName, TreeNodePage.DEFAULT_PAGE_SIZE);
        this.btreeConf = btreeConf;
    }

    @Override
    public TreeKeyNode getPage(long pageId) throws IOException {
        ByteBuffer bb = readBB(pageId);
        if (bb.getShort() == 0) { // if page is doesn't exist)
            return new TreeKeyNode(pageId, btreeConf);
        } else {
            bb.position(0);
            return new TreeKeyNode(bb, btreeConf);
        }
    }

}
