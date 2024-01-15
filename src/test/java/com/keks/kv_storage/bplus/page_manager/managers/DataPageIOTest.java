package com.keks.kv_storage.bplus.page_manager.managers;

import com.keks.kv_storage.bplus.item.KeyValueItem;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlottedPage;
import com.keks.kv_storage.bplus.page_manager.pageio.SlottedPageIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;


class DataPageIOTest {

    @Test
    public void test1(@TempDir Path dir) throws IOException {
        SlottedPageIO dataPageManager = new SlottedPageIO(dir.toFile(), "data.db");

        {
            SlottedPage slottedPage0 = new SlottedPage(0);
            for (int i = 0; i < 20; i++) {
                String key = "key" + i;
                String value = "value" + i;
                slottedPage0.addItem(new KeyValueItem(key, value));
            }


            SlottedPage slottedPage1 = new SlottedPage(1);
            for (int i = 20; i < 40; i++) {
                String key = "key" + i;
                String value = "value" + i;
                slottedPage1.addItem(new KeyValueItem(key, value));
            }


            SlottedPage slottedPage2 = new SlottedPage(2);
            for (int i = 40; i < 60; i++) {
                String key = "key" + i;
                String value = "value" + i;
                slottedPage2.addItem(new KeyValueItem(key, value));
            }

            dataPageManager.flush(slottedPage1);
            dataPageManager.flush(slottedPage0);
            dataPageManager.flush(slottedPage2);
        }
        {
            SlottedPage pageFromDisk0 = dataPageManager.getPage(0);
            for (short i = 0; i < 20; i++) {
                String key = "key" + i;
                String value = "value" + i;
                KeyValueItem keyValueItem = new KeyValueItem(pageFromDisk0.getBytesReadOnly(i));
                assertEquals(key, keyValueItem.key);
                assertEquals(value, keyValueItem.value);
            }

            SlottedPage pageFromDisk1 = dataPageManager.getPage(1);
            for (short i = 20; i < 40; i++) {
                String key = "key" + i;
                String value = "value" + i;
                KeyValueItem keyValueItem = new KeyValueItem(pageFromDisk1.getBytesReadOnly((short) (i - 20)));
                assertEquals(key, keyValueItem.key);
                assertEquals(value, keyValueItem.value);
            }

            SlottedPage pageFromDisk2 = dataPageManager.getPage(2);
            for (short i = 40; i < 60; i++) {
                String key = "key" + i;
                String value = "value" + i;
                KeyValueItem keyValueItem = new KeyValueItem(pageFromDisk2.getBytesReadOnly((short) (i - 40)));
                assertEquals(key, keyValueItem.key);
                assertEquals(value, keyValueItem.value);
            }
        }

    }

}