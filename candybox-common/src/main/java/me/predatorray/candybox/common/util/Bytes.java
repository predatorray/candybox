package me.predatorray.candybox.common.util;

import java.util.Comparator;

/** Byte-array helpers shared across modules, notably unsigned lexicographic comparison. */
public final class Bytes {

    /** Unsigned lexicographic order — the canonical ordering for keys in SSTables and merges. */
    public static final Comparator<byte[]> LEXICOGRAPHIC = Bytes::compare;

    private Bytes() {
    }

    /** Compares two arrays byte-by-byte treating each byte as unsigned (0..255). */
    public static int compare(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return a.length - b.length;
    }
}
