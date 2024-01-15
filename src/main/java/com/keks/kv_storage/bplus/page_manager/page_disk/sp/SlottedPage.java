package com.keks.kv_storage.bplus.page_manager.page_disk.sp;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.bplus.item.KvRecordSplit;
import com.keks.kv_storage.Item;
import com.keks.kv_storage.bplus.page_manager.Page;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;


/**
 * HEADER
 * ......
 * slot table                                                                                    slot item
 * v                                                                                             v
 * [PageType][PageId][NumberOfSlots][NumberOfDeletedSlots][FreeSpace][HighWatermark]................[      ] [      ]
 * ^              ^
 * firstAvailPos  highWaterMark
 */
public class SlottedPage extends Page {

    public final short pageType = SLOTTED_PAGE_TYPE;

    private short numberOfSlots;

    private short numberOfDeletedSlots;

    private int freeSpace;

    private int highWaterMark;

    private final byte[] data;

    private final ArrayList<Slot> slotsArr;

    public final int MIN_ITEM_LEN = 50;

    public static final int DEFAULT_PAGE_SIZE = 8 * 1024; // 8192
//    public static final int DEFAULT_PAGE_SIZE = 200;

    private static final int PAGE_TYPE_OVERHEAD = TypeSize.SHORT;
    private static final int ID_OVERHEAD = TypeSize.LONG;
    private static final int SLOTS_NUMBER_OVERHEAD = TypeSize.SHORT;
    private static final int SLOTS_DELETED_OVERHEAD = TypeSize.SHORT;
    private static final int FREE_SPACE_OVERHEAD = TypeSize.INT;
    private static final int HIGH_WATERMARK_OVERHEAD = TypeSize.INT;
    private static final int HEADER_OVERHEAD = PAGE_TYPE_OVERHEAD
            + ID_OVERHEAD
            + SLOTS_NUMBER_OVERHEAD
            + SLOTS_DELETED_OVERHEAD
            + FREE_SPACE_OVERHEAD
            + HIGH_WATERMARK_OVERHEAD;

    public SlottedPage(long pageId) {
        super(pageId);
        this.numberOfSlots = 0;
        this.numberOfDeletedSlots = 0;
        this.freeSpace = DEFAULT_PAGE_SIZE - HEADER_OVERHEAD;
        this.highWaterMark = DEFAULT_PAGE_SIZE;
        this.data = new byte[DEFAULT_PAGE_SIZE];
        this.slotsArr = new ArrayList<>();
    }

    public SlottedPage(ByteBuffer bb) {
        super(bb);
        this.numberOfSlots = bb.getShort();
        this.numberOfDeletedSlots = bb.getShort();
        this.freeSpace = bb.getInt();
        this.highWaterMark = bb.getInt();
        this.data = new byte[DEFAULT_PAGE_SIZE];
        this.slotsArr = new ArrayList<>(numberOfSlots);
        for (int i = 0; i < numberOfSlots; i++) {
            Slot slot = new Slot(bb);
            slotsArr.add(slot);
        }
        bb.position(0);
        bb.get(data);
    }

    public int getNumberOfItems() {
        return numberOfSlots;
    }

    public int getNumberOfDeletedItems() {
        return numberOfDeletedSlots;
    }

    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.putShort(pageType);
        bb.putLong(pageId);
        bb.putShort(numberOfSlots);
        bb.putShort(numberOfDeletedSlots);
        bb.putInt(freeSpace);
        bb.putInt(highWaterMark);
        for (int slotNumber = 0; slotNumber < numberOfSlots; slotNumber++) {
            Slot slot = slotsArr.get(slotNumber);
            slot.store(bb);
        }
        return data;
    }

    public boolean isSlotDeleted(short slotNo) {
        return slotsArr.get(slotNo).isDeleted();
    }

    public short getNextSlotId() {
        return numberOfSlots;
    }

    public int getItemLength(short slotNo) {
        return slotsArr.get(slotNo).getItemLength();
    }

    public short addItem(Item item) {
        int itemDataLen = item.getLen();
        if (getFreeSpace() < itemDataLen)
            throw new RuntimeException("Not enough space to insert slot. Available: " + getFreeSpace() + "  RequiredItemDataLen: " + itemDataLen);

        Slot slot = new Slot(highWaterMark - itemDataLen, itemDataLen);
        slotsArr.add(numberOfSlots, slot);
        numberOfSlots++;
        freeSpace -= (itemDataLen + Slot.SIZE);
        highWaterMark -= itemDataLen;
        ByteBuffer bb = ByteBuffer.wrap(data, slot.getItemStartPos(), slot.getItemLength());
        item.copyToBB(bb);
        return (short) (numberOfSlots - 1);
    }

    public void delete(short slotId) {
        if (isSlotDeleted(slotId)) return;
        assert numberOfSlots == slotsArr.size();
        if (slotId == numberOfSlots - 1) {
            Slot oldSlot = slotsArr.get(slotId);
            oldSlot.setIsDeleted();
            numberOfDeletedSlots++;
            while (oldSlot.isDeleted()) {
                slotsArr.remove(slotId);
                freeSpace = freeSpace + oldSlot.getItemLength() + Slot.SIZE;
                highWaterMark = highWaterMark + oldSlot.getItemLength();
                slotId--;
                numberOfDeletedSlots--;
                numberOfSlots--;
                if (slotId == -1) {
                    assert numberOfSlots == 0;
                    assert numberOfDeletedSlots == 0;
                    break;
                }
                oldSlot = slotsArr.get(slotId);
            }
        } else {
            numberOfDeletedSlots++;
            slotsArr.get(slotId).setIsDeleted();
        }
        assert numberOfSlots == slotsArr.size();
    }

    public ByteBuffer getBytesReadOnly(short slotId) {
        if (slotId >= slotsArr.size()) {
            System.out.println("Thread[" + Thread.currentThread().getId() + "]  "
                    + "Page: " + pageId + " doesn't contain slot: " + slotId + ". Available number of slots: " + numberOfSlots);
            throw new IndexOutOfBoundsException(); // TODO create slottedpageException
        }
        try {
            Slot slot = slotsArr.get(slotId);
            if (slot.getItemStartPos() == -1) {
                ByteBuffer bb = ByteBuffer.allocate(2);
                bb.putShort((short) 0);
                bb.position(0);
                return bb;
//                System.out.println("Thread[" + Thread.currentThread().getId() + "]  "
//                        + "Page: " + pageId + " slot: " + slotId + ". slot is empty");
//                throw new IndexOutOfBoundsException(); // TODO create slottedpageException
            }
            ByteBuffer bb = ByteBuffer.wrap(data, slot.getItemStartPos(), slot.getItemLength()).asReadOnlyBuffer();
            bb.mark();
            return bb;
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Thread[" + Thread.currentThread().getId() + "]  "
                    + "Page: " + pageId + " doesn't contain slot: " + slotId + ". Available number of slots: " + numberOfSlots);
            throw e;
        }
    }

    public ByteBuffer getBytesReadWrite(short slotId) {
        try {
            Slot slot = slotsArr.get(slotId);
            ByteBuffer bb = ByteBuffer.wrap(data, slot.getItemStartPos(), slot.getItemLength());
            bb.mark();
            return bb;
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Thread[" + Thread.currentThread().getId() + "]  "
                    + "Page: " + pageId + " doesn't contain slot: " + slotId + ". Available number of slots: " + numberOfSlots);
            throw e;
        }
    }

    // TODO Test it
    public void update(short slotId, Item newItem) {
        int newItemLen = newItem.getLen();
        if (!canUpdate(slotId, newItemLen)) throw new RuntimeException("Not enough space to update slot");
        int currentLen = getItemLength(slotId);
        Slot oldSlot = slotsArr.get(slotId);
        if (slotId == numberOfSlots - 1) {
            highWaterMark = highWaterMark + currentLen - newItemLen;
            Slot slot = new Slot(highWaterMark, newItemLen);
            slotsArr.set(slotId, slot);
            ByteBuffer bb = ByteBuffer.wrap(data, slot.getItemStartPos(), slot.getItemLength());
            newItem.copyToBB(bb);
            freeSpace = freeSpace + currentLen - newItemLen;
        } else {
            Slot slot = new Slot(oldSlot.getItemStartPos(), newItemLen);
            slotsArr.set(slotId, slot);
            ByteBuffer bb = ByteBuffer.wrap(data, slot.getItemStartPos(), slot.getItemLength());
            newItem.copyToBB(bb);
        }

    }

    public boolean canUpdate(short slotId, int newItemLen) {
        int currentLen = getItemLength(slotId);
        if (slotId == numberOfSlots - 1) {
            return getFreeSpace() + Slot.SIZE + currentLen >= newItemLen;
        } else {
            return currentLen >= newItemLen;
        }
    }

    public boolean canStore(int newItemLen) {
        return getFreeSpace() - newItemLen >= 0;
    }

    public int getFreeSpace() {
        int space = Math.max(freeSpace - Slot.SIZE, 0);
        return (space < MIN_ITEM_LEN) ? 0 : space;
    }

    @Override
    public boolean isFull() {
        return getFreeSpace() == 0;
    }

    public String dump() {
        StringBuilder sb = new StringBuilder();
        sb.append("PageSize: ").append(DEFAULT_PAGE_SIZE)
                .append("  PageId: ").append(pageId)
                .append("  NumberOfSlots: ").append(numberOfSlots)
                .append("  NumberOfDeletedSlots: ").append(numberOfDeletedSlots)
                .append("  FreeSpace: ").append(getFreeSpace())
                .append("  FirstSlotStartPos: ").append(HEADER_OVERHEAD)
                .append("  NextSlotStartPos: ").append(HEADER_OVERHEAD + numberOfSlots * Slot.SIZE)
                .append("  FreeRange: ").append(HEADER_OVERHEAD + numberOfSlots * Slot.SIZE + Slot.SIZE)
                .append(" - ")
                .append(highWaterMark)
                .append("  HighWatermark: ").append(highWaterMark)
                .append("\n")
                .append("SLOTS:\n");
        for (int i = 0; i < slotsArr.size(); i++) {
            Slot slot = slotsArr.get(i);
            sb.append(HEADER_OVERHEAD + i * Slot.SIZE).append(" : SlotId=").append(i).append(" ").append(slot).append("\n");
        }
        sb.append("\n")
                .append("ITEMS:\n");
        for (int i = slotsArr.size() - 1; 0 <= i; i--) {
            Slot slot = slotsArr.get(i);
            if (slot.isDeleted()) {
                String s = slot.getItemStartPos() + " : SlotId=" + i + " NULL";
                sb.append(s).append("\n");
            } else {
                ByteBuffer bb = ByteBuffer.wrap(data, slot.getItemStartPos(), slot.getItemLength()).asReadOnlyBuffer();
                bb.mark();
                KvRecordSplit block = new KvRecordSplit(bb);
                String s = slot.getItemStartPos() + " : " + "SlotId=" + i + "   " + block;
                sb.append(s).append("\n");
            }

        }
        return sb.toString();
    }

    private void optimizeItems() {
        highWaterMark = DEFAULT_PAGE_SIZE;
        assert numberOfSlots == slotsArr.size();
        for (short slotId = 0; slotId < slotsArr.size(); slotId++) {
            Slot slot = slotsArr.get(slotId);
            if (slot.isDeleted()) {
                Slot newSlot = new Slot(-1, 0);
                slotsArr.set(slotId, newSlot);
            } else {
                int itemLen = slot.getItemLength();
                assert itemLen > 0;
                highWaterMark -= itemLen;
                System.arraycopy(data, slot.getItemStartPos(), data, highWaterMark, itemLen);
                Slot newSlot = new Slot(highWaterMark, itemLen);
                slotsArr.set(slotId, newSlot);
            }
        }
        assert numberOfSlots == slotsArr.size();
    }

    public void tryToOptimize() {
        if (numberOfDeletedSlots > 0) {
            optimizeItems();
            freeSpace = highWaterMark - slotsArr.size() * Slot.SIZE - HEADER_OVERHEAD;
            assert highWaterMark == slotsArr.get(numberOfSlots - 1).getItemStartPos();
        }
    }

    public HashMap<Short, Short> defragmentation() {
        HashMap<Short, Short> oldToNewSlotIdsMap = new HashMap<>();
        optimizeItems();
        short x = 0;
        Deque<Short> slotsToDelete = new ArrayDeque<>();
        for (short i = 0; i < slotsArr.size(); i++) {
            Slot slot = slotsArr.get(i);
            if (slot.isDeleted()) {
                x++;
                slotsToDelete.push(i);
            } else {
                if (x != 0) oldToNewSlotIdsMap.put(i, (short) (i - x));
            }
        }
        for (Short slotId : slotsToDelete) {
            slotsArr.remove((int) slotId);
            numberOfSlots--;
        }
        numberOfDeletedSlots = 0;

        freeSpace = highWaterMark - slotsArr.size() * Slot.SIZE - HEADER_OVERHEAD;
        return oldToNewSlotIdsMap;
    }


}
