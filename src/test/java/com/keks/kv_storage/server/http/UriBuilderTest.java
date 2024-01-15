package com.keks.kv_storage.server.http;


import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;


class UriBuilderTest {

    @Test
    public void test1() {
        UriBuilder builder = new UriBuilder("http", "keks.server", 8080)
                .setPathSegments("a")
                .setParameter("hello", "world");
        assertEquals("http://keks.server:8080/a?hello=world", builder.toString());
    }

    @Test
    public void test2() {
        UriBuilder builder = new UriBuilder("http", "keks.server", 8080)
                .setPathSegments("a", "b")
                .setParameter("hello1", "world1")
                .setParameter("hello2", "world2");
        assertEquals("http://keks.server:8080/a/b?hello1=world1&hello2=world2", builder.toString());
    }

    @Test
    public void test3() {
        UriBuilder builder = new UriBuilder("http", "keks.server", 8080)
                .setPathSegments("a", "b")
                .setParameter("hello1", "world1")
                .setParameter("hello1", "world2");
        assertEquals("http://keks.server:8080/a/b?hello1=world2", builder.toString());
    }

    @Test
    public void test4() {
        UriBuilder builder = new UriBuilder("http", "keks.server", 8080)
                .setPathSegments("a", "b")
                .addParameter("hello1", "world1")
                .addParameters(new HashMap<>(){{
                    put("key1", "value1");
                }})
                .setParameter("key1", "value3");
        assertEquals("http://keks.server:8080/a/b?hello1=world1&key1=value3", builder.toString());
    }

    @Test
    public void test9() throws URISyntaxException {
        UriBuilder builder = new UriBuilder("http", "keks.server", 8080)
                .setPathSegments("a", "b")
                .setParameter("hello1", "world1")
                .setParameter("hello1", "world2");
//        assertEquals("http://keks.server:8080/a/b?hello1=world2", builder.toString());
        URI uri = builder.build();
//        System.out.println(uri.getPath());
        String[] split = uri.getPath().split("/");
//        System.out.println(Arrays.toString(split));
//        System.out.println(split[1]);
    }

}