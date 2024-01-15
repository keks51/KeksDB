package com.keks.kv_storage.lsm.ss_table;


import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SStablesList implements Iterable<SSTable> {

    private final ReentrantLock headLock = new ReentrantLock();

    private final SStableNode head = new SStableNode(null, null, null);


    public SStablesList() {

    }

    public SStablesList(LinkedList<SSTable> list) {
        for (SSTable elem : list) {
            addNodeFront(elem);
        }
    }


    @Override
    public Iterator<SSTable> iterator() {
        return new Iterator<>() {
            private SStableNode prevElem = head; // head is always null and final

            @Override
            public boolean hasNext() {
                return prevElem.next != null;
            }

            @Override
            public SSTable next() {
                prevElem.lockRead();
                SStableNode next = prevElem.next;
                prevElem.unlockRead();
                prevElem = next;
                return next.ssTable;
            }
        };
    }


    public void addNodeFront(SSTable val) {
        headLock.lock();
        head.lockWrite();
        try {
            if (head.next == null) {
                SStableNode temp = new SStableNode(val, head.next, head);
                head.next = temp;
            } else {
                SStableNode headNext = head.next;
                headNext.lockWrite();
                SStableNode temp = new SStableNode(val, headNext, head);
                headNext.prev = temp;
                head.next = temp;
                headNext.unlockWrite();
            }
        } finally {
            head.unlockWrite();
            headLock.unlock();
        }
    }


    public void replaceAndSetAsNewTail(SSTable oldElem, SSTable newElem) {
        headLock.lock();
        SStableNode prevNode = head;
        SStableNode node;
        boolean success = false;
        while (!success) {
            boolean found = false;
            while (prevNode.next != null) {
                node = prevNode.next;
                if (node.ssTable.id == oldElem.id && node.ssTable.version == oldElem.version) {
                    found = true;
                    prevNode.lockWrite();
                    node.lockWrite();
                    if (node.ssTable.id == oldElem.id && node.ssTable.version == oldElem.version) {
                        SStableNode temp = new SStableNode(newElem, null, prevNode);
                        prevNode.next = temp;
                        success = true;
                    }
                    node.unlockWrite();
                    prevNode.unlockWrite();
                    break;
                }
                prevNode = node;
            }
            if (!found) return;
        }
        headLock.unlock();
    }

    public SStableNode getHead() {
        try {
            head.lockRead();
            return head.next;
        } finally {
            head.unlockRead();
        }
    }

    public SStableNode getTail() {
        SStableNode elem = head;
        while (elem.next != null) {
            elem = elem.next;
        }
        return elem;
    }

    public void clear() {
        headLock.lock();
        this.head.next = null;
        headLock.unlock();
    }

    public LinkedList<SSTable> toJavaList() {
        LinkedList<SSTable> list = new LinkedList<>();
        SStableNode elem = head;
        while (elem != null) {
            list.addLast(elem.ssTable);
            elem = elem.next;
        }
        return list;
    }

}

