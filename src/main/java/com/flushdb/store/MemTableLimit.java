package com.flushdb.store;

import java.io.IOException;
import java.nio.file.*;
import java.util.TreeMap;
import java.util.stream.Stream;

public class MemTableLimit {

    public static void limitReached(TreeMap<String, String> memTable) throws IOException {

        if (memTable.size() <= 10) {
            return;
        }

        Path dbFolder = Path.of("./data/db");

        if (Files.notExists(dbFolder)) {
            Files.createDirectories(dbFolder);
        }

        int maxFileIndex = 0;

        try (Stream<Path> files = Files.list(dbFolder)) {
            for (Path file : files.toList()) {

                String fileName = file.getFileName().toString();

                if (fileName.startsWith("sstable_") && fileName.endsWith(".dat")) {

                    String indexPart = fileName
                            .replace("sstable_", "")
                            .replace(".dat", "");

                    int currentIndex = Integer.parseInt(indexPart);
                    maxFileIndex = Math.max(maxFileIndex, currentIndex);
                }
            }
        }

        int newIndex = maxFileIndex + 1;
        Path newFile = dbFolder.resolve("sstable_" + newIndex + ".dat");

        Files.createFile(newFile);

        StringBuilder sb = new StringBuilder();

        for (var entry : memTable.entrySet()) {
            sb.append(entry.getKey())
                    .append(":")
                    .append(entry.getValue())
                    .append("\n");
        }

        Files.writeString(
                newFile,
                sb.toString(),
                StandardOpenOption.WRITE
        );

        BloomFilter bloomFilter = new BloomFilter(
                memTable.size(),
                0.01d
        );

        for (var entry : memTable.entrySet()) {
            bloomFilter.add(entry.getKey());
        }

        Path bloomFile = dbFolder.resolve("sstable_" + newIndex + ".bloom");
        bloomFilter.writeToFile(bloomFile);

        System.out.println("Flushed MemTable to: " + newFile);

        memTable.clear();
        LogManager.clean();
    }
}
