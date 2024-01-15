package com.keks.kv_storage.bplus.page_manager;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.bplus.page_manager.page_disk.fixed.TreeNodePage;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class BplusTreeRuntimeParameters {

    public static final String TREE_PROPERTIES_FILE_NAME = "runtime-parameters.db";

    private final MappedByteBuffer out;
    private final int TREE_ORDER_START_POS = 0;
    private final int ROOT_PAGE_ID_START_POS = TypeSize.INT + TREE_ORDER_START_POS;
    private final int MOST_LEFT_LEAF_PAGE_ID_START_POS = TypeSize.LONG + ROOT_PAGE_ID_START_POS;
    private final int TREE_HEIGHT_START_POS = TypeSize.LONG + MOST_LEFT_LEAF_PAGE_ID_START_POS;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile long rootPageId = 1L;
    private volatile long mostLeafPageId = 1L;
    private volatile int treeHeight;
    private final int btreeOrder;

    private final FileChannel fileChannel;

    public BplusTreeRuntimeParameters(int btreeOrder, File dir) throws IOException {
        File propertiesFile = new File(dir, TREE_PROPERTIES_FILE_NAME);
        this.fileChannel = FileChannel.open(propertiesFile.toPath(),
                StandardOpenOption.SYNC, // DSYNC not always persist data
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ);
        this.out = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, TreeNodePage.DEFAULT_PAGE_SIZE);
        this.btreeOrder = btreeOrder;
        setBtreeOrder(btreeOrder);
        setRootPageId(-1L);
        setMostLeftLeafPageId(-1L);
    }

    public BplusTreeRuntimeParameters(File dir) throws IOException {
        File propertiesFile = new File(dir, TREE_PROPERTIES_FILE_NAME);
        this.fileChannel = FileChannel.open(propertiesFile.toPath(),
                StandardOpenOption.SYNC, // DSYNC not always persist data
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);
        this.out = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, TreeNodePage.DEFAULT_PAGE_SIZE);

        out.position(TREE_ORDER_START_POS);
        this.btreeOrder = out.getInt();

        out.position(ROOT_PAGE_ID_START_POS);
        this.rootPageId = out.getLong();

        out.position(MOST_LEFT_LEAF_PAGE_ID_START_POS);
        this.mostLeafPageId = out.getLong();

        out.position(TREE_HEIGHT_START_POS);
        this.treeHeight = out.getInt();
    }

    private void setBtreeOrder(int order) {
        lock.writeLock().lock();
        out.position(TREE_ORDER_START_POS);
        out.putInt(order);
        lock.writeLock().unlock();
    }

    public int getBtreeOrder() {
        lock.readLock().lock();
        try {
            return btreeOrder;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void incTreeHeight() {
        lock.writeLock().lock();
        out.position(TREE_HEIGHT_START_POS);
        treeHeight = getTreeHeight() + 1;
        out.putInt(treeHeight);
        lock.writeLock().unlock();
    }

    public void decTreeHeight() {
        lock.writeLock().lock();
        out.position(TREE_HEIGHT_START_POS);
        treeHeight = getTreeHeight() - 1;
        out.putInt(treeHeight);
        lock.writeLock().unlock();
    }

    public int getTreeHeight() {
        lock.readLock().lock();
        try {
            return treeHeight;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setRootPageId(long pageId) {
        lock.writeLock().lock();
        out.position(ROOT_PAGE_ID_START_POS);
        out.putLong(pageId);
        rootPageId = pageId;
        lock.writeLock().unlock();
    }

    public boolean rootExist() {
        lock.readLock().lock();
        try {
            return rootPageId > 0;
        } finally {
            lock.readLock().unlock();
        }

    }

    public long getRootPageId() {
        lock.readLock().lock();
        try {
            return rootPageId;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setMostLeftLeafPageId(long pageId) {
        lock.writeLock().lock();
        out.position(MOST_LEFT_LEAF_PAGE_ID_START_POS);
        out.putLong(pageId);
        mostLeafPageId = pageId;
        lock.writeLock().unlock();
    }

    public long getMostLeftLeafPageId() {
        lock.readLock().lock();
        try {
            return mostLeafPageId;
        } finally {
            lock.readLock().unlock();
        }

    }

    public void close() throws IOException {
        fileChannel.close();
    }

}
