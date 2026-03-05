package com.flushdb.store;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class Utils {

    static void writeToLog(Path path, String data) {
        Path fileName = Paths.get(path.toUri());
        try {
            Files.writeString(
                fileName,
                data,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.SYNC
            );
            System.out.println("Data written to log file");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static Integer searchKeyInSSTable(Path path, String key)
        throws IOException {
        try (Stream<String> lines = Files.lines(path)) {
            for (String line : (Iterable<String>) lines::iterator) {
                String[] kv = line.split(":", 2);

                if (kv.length != 2) continue;

                String k = kv[0].trim();
                String v = kv[1].trim();

                if (k.equals(key)) {
                    if (v.equals("__tomb__")) {
                        return -1;
                    }

                    try {
                        return Integer.parseInt(v);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
            }
        }

        return null;
    }

    static int maxFileIndexCounter(Path path) throws IOException {
        int maxFileIndex = 0;

        try (Stream<Path> files = Files.list(path)) {
            for (Path file : files.toList()) {
                String fileName = file.getFileName().toString();

                if (
                    fileName.startsWith("sstable_") && fileName.endsWith(".dat")
                ) {
                    String indexPart = fileName
                        .replace("sstable_", "")
                        .replace(".dat", "");

                    int currentIndex = Integer.parseInt(indexPart);
                    maxFileIndex = Math.max(maxFileIndex, currentIndex);
                }
            }
        }
        return maxFileIndex;
    }

    static boolean bloomExistsAndSaysNo(Path bloomPath, String key) {
        if (Files.notExists(bloomPath)) {
            return false;
        }

        try {
            BloomFilter bloomFilter = BloomFilter.readFromFile(
                bloomPath
            );
            return !bloomFilter.mightContain(key);
        } catch (IOException e) {
            return false;
        }
    }
}
