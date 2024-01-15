package com.keks.kv_storage.bplus.tree.utils;


public class BtreeUtils {

    /**
     * 1 -> 0
     * 2 -> 0
     * 3 -> 1
     * 4 -> 1
     * 5 -> 2
     * 6 -> 2
     * 7 -> 3
     * 8 -> 3
     * [0 1 2 3 4]
     * | midpoint
     * [0 1 2 3 4 5]
     * | midpoint
     * [0 1 2 3 4 5 6]
     * | midpoint
     * [0 1 2 3 4 5 6 7]
     * | midpoint
     *
     * @return
     */
    public static int getArrMidpoint(int arrSize) {
        return (int) Math.ceil((arrSize + 1) / 2.0) - 1;
    }

}
