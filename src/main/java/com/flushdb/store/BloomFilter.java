package com.flushdb.store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

class BloomFilter {

    private static final int MAGIC = 0x424C4F4D; 
    private static final int VERSION = 1;

    BitSet bits;
    int m;
    int k;

    BloomFilter(int expectedInsertions, double falsePositiveRate) {
        int safeN = Math.max(expectedInsertions, 1);
        double safeP = Math.min(Math.max(falsePositiveRate, 0.0001d), 0.9999d);

        m = Math.max(
            8,
            (int) Math.ceil((-safeN * Math.log(safeP)) / (Math.pow(Math.log(2d), 2d)))
        );
        k = Math.max(1, (int) Math.round((m / (double) safeN) * Math.log(2d)));
        bits = new BitSet(m);
    }

    private BloomFilter(BitSet bits, int m, int k) {
        this.bits = bits;
        this.m = m;
        this.k = Math.max(k, 1);
    }

    void add(String key) {
        if (key == null) return;

        int[] indices = indicesFor(key);
        for (int idx : indices) {
            bits.set(idx);
        }
    }

    boolean mightContain(String key) {
        if (key == null) return false;

        int[] indices = indicesFor(key);
        for (int idx : indices) {
            if (!bits.get(idx)) {
                return false;
            }
        }
        return true;
    }

    void writeToFile(Path filePath) throws IOException {
        byte[] bitBytes = bits.toByteArray();

        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(filePath))) {
            dos.writeInt(MAGIC);
            dos.writeInt(VERSION);
            dos.writeInt(m);
            dos.writeInt(k);
            dos.writeInt(bitBytes.length);
            dos.write(bitBytes);
        }
    }

    static BloomFilter readFromFile(Path filePath) throws IOException {
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(filePath))) {
            int magic = dis.readInt();
            int version = dis.readInt();
            int m = dis.readInt();
            int k = dis.readInt();
            int bytesLength = dis.readInt();

            if (magic != MAGIC || version != VERSION || m <= 0 || bytesLength < 0) {
                throw new IOException("Invalid bloom filter file: " + filePath);
            }

            byte[] bitBytes = new byte[bytesLength];
            dis.readFully(bitBytes);
            return new BloomFilter(BitSet.valueOf(bitBytes), m, k);
        }
    }

    private int[] indicesFor(String key) {
        int[] indices = new int[k];
        int hash1 = mixHash(key.hashCode());
        int hash2 = mixHash(Integer.rotateLeft(hash1, 16) ^ 0x9E3779B9);

        if (hash2 == 0) {
            hash2 = 0x7F4A7C15;
        }

        for (int i = 0; i < k; i++) {
            long combined = (hash1 & 0xffffffffL) + (long) i * hash2;
            indices[i] = (int) Math.floorMod(combined, m);
        }
        return indices;
    }

    private int mixHash(int value) {
        int h = value;
        h ^= (h >>> 16);
        h *= 0x7feb352d;
        h ^= (h >>> 15);
        h *= 0x846ca68b;
        h ^= (h >>> 16);
        return h;
    }
}
