package com.keks.kv_storage.utils;


public class EnvVar {

    public static String getEnvVar(String name) {
        String env = System.getenv(name);
        if (env == null) throw new IllegalArgumentException("Env var: " + name + " is not passed");
        return env;
    }

}
