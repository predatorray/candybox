package me.predatorray.candybox.coordination;

/**
 * Canonical coordination key/resource names, shared by the server (which owns Boxes) and the client
 * (which routes to owners) so both agree on where the ownership lease and manifest pointer live.
 */
public final class CandyboxKeys {

    private CandyboxKeys() {
    }

    /** The ownership-lease resource for a Box. The lease holder is the Box's owner. */
    public static String ownerResource(String boxName) {
        return "boxes/" + boxName + "/owner";
    }

    /** The versioned key holding the pointer to a Box's current manifest ledger. */
    public static String manifestKey(String boxName) {
        return "boxes/" + boxName + "/manifest";
    }
}
