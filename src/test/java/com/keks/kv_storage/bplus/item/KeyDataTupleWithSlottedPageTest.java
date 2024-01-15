package com.keks.kv_storage.bplus.item;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlottedPage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;


class KeyDataTupleWithSlottedPageTest {

    @Test
    public void test1() {
        String key = "key1";
        String value673Len = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. In dignissim imperdiet ullamcorper. Praesent quis vehicula mi. Sed ac ultrices velit, vitae finibus urna. Vestibulum dignissim pulvinar ex vel suscipit. Integer sit amet lorem ut justo consectetur vulputate. Nunc molestie ligula at arcu porttitor aliquam. Vivamus sagittis cursus ornare. In a sem ac tortor facilisis lacinia sit amet a massa. Maecenas ultricies ullamcorper magna, quis fermentum sem semper eget. Suspendisse viverra pharetra ante vitae vulputate. Vestibulum sed nisi tellus. Morbi lorem ipsum, cursus at mauris ut, tincidunt laoreet libero. Mauris sed mauris ac dolor blandit suscipit eget id dolor.";

        String value25574Len = value673Len.repeat(38);
//        System.out.println(value25574Len.length());

        KVRecord recordToSplit = new KVRecord(key, value25574Len.getBytes());
        int tupleLen = recordToSplit.getLen();
        ArrayList<SlottedPage> pages = new ArrayList<>();
        KVRecordSplitter splitter = new KVRecordSplitter(recordToSplit);
        int previousPageId = -1;

        do {
            SlottedPage slottedPage = new SlottedPage(previousPageId + 1);
            KvRecordSplit block = splitter.nextSplit(slottedPage.getFreeSpace());
            slottedPage.addItem(block);
            pages.add(slottedPage);
            previousPageId++;
        } while (splitter.hasNextSplit());

        KVRecordSplitsBuilder recordSplitsBuilder = new KVRecordSplitsBuilder(tupleLen);

        pages.forEach(page -> recordSplitsBuilder.add(page.getBytesReadOnly((short) 0)));

        KVRecord keyDataTuple = recordSplitsBuilder.buildKVRecord();


        assertEquals(key, keyDataTuple.key);
        assertEquals(value25574Len, new String(keyDataTuple.valueBytes));
    }

}