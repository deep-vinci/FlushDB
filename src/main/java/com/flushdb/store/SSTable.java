package com.flushdb.store;

import java.io.IOException;
import java.nio.file.Path;

public class SSTable {

    public static void readSSTable(String key) throws IOException {
        int maxFileCount = Utils.maxFileIndexCounter(Path.of("./data/db"));

        for (int i = maxFileCount; i >= 1; i--) {
            Integer val = Utils.searchKeyInSSTable(
                Path.of("./data/db/sstable_" + i + ".dat"),
                key
            );

            if (val == null) continue;

            if (val == -1) {
                System.out.println("Key deleted (tombstone found)");
            } else {
                System.out.println(val);
            }
            return;
        }

        System.out.println("null");
    }
}
