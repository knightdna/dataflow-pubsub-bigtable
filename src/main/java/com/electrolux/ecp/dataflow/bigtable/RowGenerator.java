package com.electrolux.ecp.dataflow.bigtable;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class RowGenerator extends DoFn<String, Mutation> {

    @ProcessElement
    public void processElement(ProcessContext context) {
        String decodedHaclAsJsonString = context.element();
        String rowKey = String.valueOf(
                LocalDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"))
                        .toEpochSecond(ZoneOffset.UTC));
        Put put = new Put(rowKey.getBytes());
        put.addColumn("VALUE".getBytes(), "VALUE".getBytes(), decodedHaclAsJsonString.getBytes());
        context.output(put);
    }

}
