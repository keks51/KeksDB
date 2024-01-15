package com.keks.kv_storage.lsm.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.function.Function;

public class HashFunctions {

    private static final HashMap<String, Function<String, Integer>> hashFunctions = new HashMap<>(){
        {
            put("java", String::hashCode);
            put("murmur", HashFunctions::murmur);
            put("md5", HashFunctions::md5);
            put("md2", HashFunctions::md2);
        }
    };

    public static Function<String, Integer> getHashFunctionByName(String name) {
        return hashFunctions.get(name);
    }

    // https://stackoverflow.com/questions/28151014/md5-for-my-java-hashcode-which-is-int-32-bits
    public static int md5(String key){
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] byteArray = digest.digest(key.getBytes("UTF-8"));
            ByteBuffer buffer = ByteBuffer.wrap(byteArray);
            return buffer.getInt() & 0x7fffffff;
        } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public static int md2(String key){
        try {
            MessageDigest digest = MessageDigest.getInstance("MD2");
            byte[] byteArray = digest.digest(key.getBytes("UTF-8"));
            ByteBuffer buffer = ByteBuffer.wrap(byteArray);
            return buffer.getInt() & 0x7fffffff;
        } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public static int murmur(String e) {
        int length = e.length();
        byte[] data = e.getBytes();

        int m = 0x5bd1e995;
        int r = 24;

        int h = length;

        int len_4 = length >> 2;

        for (int i = 0; i < len_4; i++) {
            int i_4 = i << 2;
            int k = data[i_4 + 3];
            k = k << 8;
            k = k | (data[i_4 + 2] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 1] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 0] & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // avoid calculating modulo
        int len_m = len_4 << 2;
        int left = length - len_m;

        if (left != 0) {
            if (left >= 3) {
                h ^= (int) data[length - 3] << 16;
            }
            if (left >= 2) {
                h ^= (int) data[length - 2] << 8;
            }
            if (left >= 1) {
                h ^= (int) data[length - 1];
            }

            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }




}
