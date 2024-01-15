package com.keks.kv_storage.bplus.page_manager.page_disk.sp;

import com.keks.kv_storage.bplus.item.KeyValueItem;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;


class SlottedPageTest {

    @Test
    public void test1() {
        SlottedPage slottedPage = new SlottedPage(0);

        assertEquals(0, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8166, slottedPage.getFreeSpace());
    }

    @Test
    public void test2() {
        SlottedPage slottedPage = new SlottedPage(0);
//        System.out.println(slottedPage.dump());
        int cnt = 185;
        for (int i = 0; i < cnt; i++) {
            String key = "keykeyyy";
            String value = "valuevaluevaluevaluevaluevalue";

            slottedPage.addItem(new KeyValueItem(key, value));
        }


//        System.out.println(slottedPage.dump());
        assertEquals(185, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(0, slottedPage.getFreeSpace());
    }

    @Test
    public void test3() {
        String lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In dignissim imperdiet ullamcorper. " +
                "Praesent quis vehicula mi. Sed ac ultrices velit, vitae finibus urna. Vestibulum dignissim pulvinar " +
                "ex vel suscipit. Integer sit amet lorem ut justo consectetur vulputate. Nunc molestie ligula at arcu " +
                "porttitor aliquam. Vivamus sagittis cursus ornare. In a sem ac tortor facilisis lacinia sit amet a " +
                "massa. Maecenas ultricies ullamcorper magna, quis fermentum sem semper eget. Suspendisse viverra " +
                "pharetra ante vitae vulputate. Vestibulum sed nisi tellus. Morbi lorem ipsum, cursus at mauris ut, " +
                "tincidunt laoreet libero. Mauris sed mauris ac dolor blandit suscipit eget id dolor. Sed id felis " +
                "velit. Quisque posuere orci neque, ut ullamcorper dolor pellentesque sed. Pellentesque feugiat non " +
                "ante in porttitor. Duis accumsan, sapien in pellentesque ultrices, elit velit ullamcorper metus, " +
                "vitae vulputate magna purus id leo. Aenean a felis non nibh fringilla accumsan. Sed dictum " +
                "fermentum ante eu pharetra. Cras cursus, ante nec consectetur volutpat, nisi tortor pretium risus, " +
                "ac tempor nibh tortor eget urna. Sed maximus, sapien vitae consectetur suscipit, turpis elit " +
                "egestas dui, vel venenatis turpis elit in diam. Suspendisse a quam massa. Cras vitae accumsan odio, " +
                "at faucibus turpis. Donec blandit eget mauris varius iaculis.";
        System.out.println(lorem.length());

        SlottedPage slottedPage = new SlottedPage(0);

        int cnt = 6;
        for (int i = 0; i < cnt; i++) {
            String key = "key" + i;
            slottedPage.addItem(new KeyValueItem(key, lorem));
        }
        slottedPage.addItem(new KeyValueItem("key6", "a".repeat(134)));

        for (short i = 0; i < cnt; i++) {
            KeyValueItem keyValueItem = new KeyValueItem(slottedPage.getBytesReadOnly(i));
            String key = "key" + i;
            assertEquals(key, keyValueItem.key);
            assertEquals(lorem, keyValueItem.value);
        }
        KeyValueItem lastKeyValueItem = new KeyValueItem(slottedPage.getBytesReadOnly((short) 6));
        assertEquals("key6", lastKeyValueItem.key);
        assertEquals("a".repeat(134), lastKeyValueItem.value);

        System.out.println(slottedPage.dump());
        assertEquals(7, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(0, slottedPage.getFreeSpace());
    }

    @Test
    public void test4() {
        SlottedPage slottedPage = new SlottedPage(0);

        int cnt = 20;
        for (int i = 0; i < cnt; i++) {
            String key = "key" + i;
            String value = "value" + i;
            slottedPage.addItem(new KeyValueItem(key, value));
        }

        for (short i = 0; i < cnt; i++) {
            slottedPage.delete(i);
        }

        System.out.println(slottedPage.dump());
        assertEquals(0, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8166, slottedPage.getFreeSpace());
    }

    @Test
    public void test5() {
        SlottedPage slottedPage = new SlottedPage(0);

        String key0 = "key0";
        String value0 = "value0";
        short slotId0 = slottedPage.addItem(new KeyValueItem(key0, value0));
        String key1 = "key1";
        String value1 = "value1";
        short slotId1 = slottedPage.addItem(new KeyValueItem(key1, value1));
        String key2 = "key2";
        String value2 = "value2";
        short slotId2 = slottedPage.addItem(new KeyValueItem(key2, value2));
        String key3 = "key3";
        String value3 = "value3";
        short slotId3 = slottedPage.addItem(new KeyValueItem(key3, value3));
        String key4 = "key4";
        String value4 = "value4";
        short slotId4 = slottedPage.addItem(new KeyValueItem(key4, value4));
        String key5 = "key5";
        String value5 = "value5";
        short slotId5 = slottedPage.addItem(new KeyValueItem(key5, value5));
        String key6 = "key6";
        String value6 = "value6";
        short slotId6 = slottedPage.addItem(new KeyValueItem(key6, value6));

        assertEquals(7, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8054, slottedPage.getFreeSpace());

        System.out.println(slottedPage.dump());

        slottedPage.delete(slotId0);
        slottedPage.delete(slotId4);
        slottedPage.delete(slotId5);
        System.out.println(slottedPage.dump());
        assertTrue(slottedPage.isSlotDeleted(slotId0));
        assertFalse(slottedPage.isSlotDeleted(slotId1));
        assertFalse(slottedPage.isSlotDeleted(slotId2));
        assertFalse(slottedPage.isSlotDeleted(slotId3));
        assertTrue(slottedPage.isSlotDeleted(slotId4));
        assertTrue(slottedPage.isSlotDeleted(slotId5));
        assertFalse(slottedPage.isSlotDeleted(slotId6));
        assertEquals(7, slottedPage.getNumberOfItems());
        assertEquals(3, slottedPage.getNumberOfDeletedItems());
        assertEquals(8054, slottedPage.getFreeSpace());
        assertEquals(7, slottedPage.getNextSlotId());

        slottedPage.delete(slotId6);
        System.out.println(slottedPage.dump());
        assertTrue(slottedPage.isSlotDeleted(slotId0));
        assertFalse(slottedPage.isSlotDeleted(slotId1));
        assertFalse(slottedPage.isSlotDeleted(slotId2));
        assertFalse(slottedPage.isSlotDeleted(slotId3));
        assertEquals(4, slottedPage.getNumberOfItems());
        assertEquals(1, slottedPage.getNumberOfDeletedItems());
        assertEquals(8102, slottedPage.getFreeSpace());
        assertEquals(4, slottedPage.getNextSlotId());

        slottedPage.delete(slotId0);
        slottedPage.delete(slotId1);
        slottedPage.delete(slotId2);
        slottedPage.delete(slotId3);
        assertEquals(0, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8166, slottedPage.getFreeSpace());
        assertEquals(0, slottedPage.getNextSlotId());
        System.out.println(slottedPage.dump());
    }

    @Test
    public void test6() {
        SlottedPage slottedPage = new SlottedPage(0);

        HashMap<Short, Short> defragmentation = slottedPage.defragmentation();

        System.out.println(slottedPage.dump());
        assertTrue(defragmentation.isEmpty());
        assertEquals(0, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8166, slottedPage.getFreeSpace());
        assertEquals(0, slottedPage.getNextSlotId());
    }

    @Test
    public void test7() {
        SlottedPage slottedPage = new SlottedPage(0);

        int cnt = 20;
        for (int i = 0; i < cnt; i++) {
            String key = "key" + i;
            String value = "value" + i;
            slottedPage.addItem(new KeyValueItem(key, value));
        }

        for (short i = 0; i < cnt; i++) {
            KeyValueItem keyValueItem = new KeyValueItem(slottedPage.getBytesReadOnly(i));
            String key = "key" + i;
            String value = "value" + i;
            assertEquals(key, keyValueItem.key);
            assertEquals(value, keyValueItem.value);
        }

        HashMap<Short, Short> defragmentation = slottedPage.defragmentation();

        assertTrue(defragmentation.isEmpty());
        assertEquals(20, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(7826, slottedPage.getFreeSpace());
        assertEquals(20, slottedPage.getNextSlotId());
    }

    @Test
    public void test8() {
        SlottedPage slottedPage = new SlottedPage(0);

        String key0 = "key0";
        String value0 = "value+";
        short slotId0 = slottedPage.addItem(new KeyValueItem(key0, value0));
        String key1 = "key1";
        String value1 = "value++";
        short slotId1 = slottedPage.addItem(new KeyValueItem(key1, value1));
        String key2 = "key2";
        String value2 = "value+++";
        short slotId2 = slottedPage.addItem(new KeyValueItem(key2, value2));
        String key3 = "key3";
        String value3 = "value++++";
        short slotId3 = slottedPage.addItem(new KeyValueItem(key3, value3));
        String key4 = "key4";
        String value4 = "value+++++";
        short slotId4 = slottedPage.addItem(new KeyValueItem(key4, value4));
        String key5 = "key5";
        String value5 = "value++++++";
        short slotId5 = slottedPage.addItem(new KeyValueItem(key5, value5));
        String key6 = "key6";
        String value6 = "value+++++++";
        short slotId6 = slottedPage.addItem(new KeyValueItem(key6, value6));
        System.out.println(slottedPage.dump());

        slottedPage.delete(slotId0);
        slottedPage.delete(slotId4);
        slottedPage.delete(slotId5);
        System.out.println(slottedPage.dump());

        HashMap<Short, Short> defragmentation = slottedPage.defragmentation();
        System.out.println(slottedPage.dump());
        assertEquals(4, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8090, slottedPage.getFreeSpace());
        assertEquals(4, slottedPage.getNextSlotId());
        assertEquals(key1, new KeyValueItem(slottedPage.getBytesReadOnly(slotId0)).key);
        assertEquals(key2, new KeyValueItem(slottedPage.getBytesReadOnly(slotId1)).key);
        assertEquals(key3, new KeyValueItem(slottedPage.getBytesReadOnly(slotId2)).key);
        assertEquals(key6, new KeyValueItem(slottedPage.getBytesReadOnly(slotId3)).key);
        assertEquals((short) 0, defragmentation.get(slotId1));
        assertEquals((short) 1, defragmentation.get(slotId2));
        assertEquals((short) 2, defragmentation.get(slotId3));
        assertEquals((short) 3, defragmentation.get(slotId6));
    }

    @Test
    public void test9() {
        SlottedPage slottedPage = new SlottedPage(0);
        String key = "key";
        String value = "a".repeat(8139);

        slottedPage.addItem(new KeyValueItem(key, value));
        System.out.println(slottedPage.dump());
        assertEquals(1, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(0, slottedPage.getFreeSpace());

        KeyValueItem keyValueItem = new KeyValueItem(slottedPage.getBytesReadOnly((short) 0));
        assertEquals(key, keyValueItem.key);
        assertEquals(value, keyValueItem.value);

        slottedPage.delete((short) 0);
        assertEquals(0, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8166, slottedPage.getFreeSpace());
    }

    @Test
    public void test10() {
        SlottedPage slottedPage = new SlottedPage(0);

        String key0 = "key0";
        String value0 = "value0";
        short slotId0 = slottedPage.addItem(new KeyValueItem(key0, value0));
        String key1 = "key1";
        String value1 = "value1";
        short slotId1 = slottedPage.addItem(new KeyValueItem(key1, value1));
        String key2 = "key2";
        String value2 = "value2";
        short slotId2 = slottedPage.addItem(new KeyValueItem(key2, value2));
        String key3 = "key3";
        String value3 = "value3";
        short slotId3 = slottedPage.addItem(new KeyValueItem(key3, value3));
        String key4 = "key4";
        String value4 = "value4";
        short slotId4 = slottedPage.addItem(new KeyValueItem(key4, value4));
        String key5 = "key5";
        String value5 = "value5";
        short slotId5 = slottedPage.addItem(new KeyValueItem(key5, value5));
        String key6 = "key6";
        String value6 = "value6";
        short slotId6 = slottedPage.addItem(new KeyValueItem(key6, value6));

        assertEquals(7, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8054, slottedPage.getFreeSpace());



        KeyValueItem kvSlot1Update1 = new KeyValueItem("key7", "val7");
        assertTrue(slottedPage.canUpdate(slotId1, kvSlot1Update1.getLen()));
        slottedPage.update(slotId1, new KeyValueItem("key7", "val7"));
        assertFalse(slottedPage.canUpdate(slotId1, 11));
        assertEquals(kvSlot1Update1.key, new KeyValueItem(slottedPage.getBytesReadOnly(slotId1)).key);
        assertEquals(10, slottedPage.getItemLength(slotId1));

        assertEquals(8054, slottedPage.getFreeSpace());
        System.out.println(slottedPage.dump());
        HashMap<Short, Short> defragmentation = slottedPage.defragmentation();
        assertTrue(defragmentation.isEmpty());
        System.out.println(slottedPage.dump());
        assertEquals(8056, slottedPage.getFreeSpace());




        KeyValueItem kvSlot1Update2 = new KeyValueItem("key8", "value8");
        assertFalse(slottedPage.canUpdate(slotId1, kvSlot1Update2.getLen()));
        assertTrue(slottedPage.canUpdate(slotId2, kvSlot1Update2.getLen()));
        slottedPage.update(slotId2, kvSlot1Update2);
        assertEquals(kvSlot1Update2.key, new KeyValueItem(slottedPage.getBytesReadOnly(slotId2)).key);
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        System.out.println(slottedPage.dump());
    }

    @Test
    public void test11() {
        SlottedPage slottedPage = new SlottedPage(0);

        String key0 = "key0";
        String value0 = "value0";
        short slotId0 = slottedPage.addItem(new KeyValueItem(key0, value0));
        String key1 = "key1";
        String value1 = "value1";
        short slotId1 = slottedPage.addItem(new KeyValueItem(key1, value1));
        String key2 = "key2";
        String value2 = "value2";
        short slotId2 = slottedPage.addItem(new KeyValueItem(key2, value2));
        String key3 = "key3";
        String value3 = "value3";
        short slotId3 = slottedPage.addItem(new KeyValueItem(key3, value3));
        String key4 = "key4";
        String value4 = "value4";
        short slotId4 = slottedPage.addItem(new KeyValueItem(key4, value4));
        String key5 = "key5";
        String value5 = "value5";
        short slotId5 = slottedPage.addItem(new KeyValueItem(key5, value5));
        String key6 = "key6";
        String value6 = "value6";
        short slotId6 = slottedPage.addItem(new KeyValueItem(key6, value6));

        assertEquals(7, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8054, slottedPage.getFreeSpace());



        KeyValueItem kvSlot6Update1 = new KeyValueItem("key7", "val7");
        assertTrue(slottedPage.canUpdate(slotId6, kvSlot6Update1.getLen()));
        slottedPage.update(slotId6, kvSlot6Update1);
        assertTrue(slottedPage.canUpdate(slotId6, 11));
        assertEquals(kvSlot6Update1.key, new KeyValueItem(slottedPage.getBytesReadOnly(slotId6)).key);
        assertEquals(10, slottedPage.getItemLength(slotId6));
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8056, slottedPage.getFreeSpace());
        System.out.println(slottedPage.dump());



        KeyValueItem kvSlot6Update2 = new KeyValueItem("key7", "val7aaaaaaaaaaaaaa");
        assertTrue(slottedPage.canUpdate(slotId6, kvSlot6Update2.getLen()));
        slottedPage.update(slotId6, kvSlot6Update2);
        assertTrue(slottedPage.canUpdate(slotId6, 11));
        assertTrue(slottedPage.canUpdate(slotId6, 80));
        assertEquals(kvSlot6Update2.key, new KeyValueItem(slottedPage.getBytesReadOnly(slotId6)).key);
        assertEquals(24, slottedPage.getItemLength(slotId6));
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8042, slottedPage.getFreeSpace());
        System.out.println(slottedPage.dump());
    }

    @Test
    public void test12() {
        SlottedPage slottedPage = new SlottedPage(0);

        String key0 = "key0";
        String value0 = "value0";
        short slotId0 = slottedPage.addItem(new KeyValueItem(key0, value0));

        slottedPage.update(slotId0, new KeyValueItem(key0, "value"));

        slottedPage.delete(slotId0);
        assertEquals(0, slottedPage.getNumberOfItems());
        assertEquals(0, slottedPage.getNumberOfDeletedItems());
        assertEquals(8166, slottedPage.getFreeSpace());
        System.out.println(slottedPage.dump());
    }

    // TODO add asserts
    @Test
    public void test13() {
        SlottedPage slottedPage = new SlottedPage(0);

        String key0 = "key0";
        String value0 = "value+";
        short slotId0 = slottedPage.addItem(new KeyValueItem(key0, value0));
        String key1 = "key1";
        String value1 = "value++";
        short slotId1 = slottedPage.addItem(new KeyValueItem(key1, value1));
        String key2 = "key2";
        String value2 = "value+++";
        short slotId2 = slottedPage.addItem(new KeyValueItem(key2, value2));
        String key3 = "key3";
        String value3 = "value++++";
        short slotId3 = slottedPage.addItem(new KeyValueItem(key3, value3));
        String key4 = "key4";
        String value4 = "value+++++";
        short slotId4 = slottedPage.addItem(new KeyValueItem(key4, value4));
        String key5 = "key5";
        String value5 = "value++++++";
        short slotId5 = slottedPage.addItem(new KeyValueItem(key5, value5));
        String key6 = "key6";
        String value6 = "value+++++++";
        short slotId6 = slottedPage.addItem(new KeyValueItem(key6, value6));
        System.out.println(slottedPage.dump());
        System.out.println("\nDeleting\n\n");

        slottedPage.delete(slotId0);
        slottedPage.delete(slotId2);
        slottedPage.delete(slotId3);
        slottedPage.delete(slotId4);
        slottedPage.delete(slotId5);

        System.out.println(slottedPage.dump());

        System.out.println("\nOptimizing\n\n");
        slottedPage.tryToOptimize();
        System.out.println(slottedPage.dump());

        System.out.println("\nDeleting slot 6\n\n");
        slottedPage.delete(slotId6);
        System.out.println(slottedPage.dump());

        assertEquals(2, slottedPage.getNumberOfItems());
        assertEquals(1, slottedPage.getNumberOfDeletedItems());
        assertEquals(8145, slottedPage.getFreeSpace());
        assertEquals(2, slottedPage.getNextSlotId());
        assertEquals(key1, new KeyValueItem(slottedPage.getBytesReadOnly(slotId1)).key);
        assertTrue(slottedPage.isSlotDeleted(slotId0));
    }

}