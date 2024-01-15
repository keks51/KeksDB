package com.keks.kv_storage.lsm.ss_table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;


public class SSTableMetadata {

    public final int sparseIndexSize;
    public final int numberOfRecordsWithDeleted;
    public final int records;
    public final int deletedRecords;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SSTableMetadata(@JsonProperty("sparseIndexSize")int sparseIndexSize,
                           @JsonProperty("numberOfRecordsWithDeleted")int numberOfRecordsWithDeleted,
                           @JsonProperty("deletedRecords")int deletedRecords) {

        this.sparseIndexSize = sparseIndexSize;
        this.numberOfRecordsWithDeleted = numberOfRecordsWithDeleted;
        this.records = numberOfRecordsWithDeleted - deletedRecords;
        this.deletedRecords = deletedRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableMetadata that = (SSTableMetadata) o;
        return  sparseIndexSize == that.sparseIndexSize
                && numberOfRecordsWithDeleted == that.numberOfRecordsWithDeleted
                && records == that.records
                && deletedRecords == that.deletedRecords;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sparseIndexSize, numberOfRecordsWithDeleted, records, deletedRecords);
    }

}
