package com.keks.kv_storage.bplus;


import java.util.Objects;


public class TableName {

    public final String name;

    public TableName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableName tableName = (TableName) o;
        return Objects.equals(name, tableName.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

}
