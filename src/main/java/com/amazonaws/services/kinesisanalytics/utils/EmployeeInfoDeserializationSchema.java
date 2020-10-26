package com.amazonaws.services.kinesisanalytics.utils;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import software.amazon.kinesis.connectors.flink.serialization.KinesisDeserializationSchema;

import java.io.IOException;

public class EmployeeInfoDeserializationSchema implements KinesisDeserializationSchema<EmployeeInfo> {
    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public EmployeeInfo deserialize(byte[] bytes,
                                    String partitionKey,
                                    String seqNum,
                                    long approximateArrivalTimestamp,
                                    String stream,
                                    String shardId) throws IOException {
        EmployeeInfo ei = objectMapper.readValue(bytes, EmployeeInfo.class);

        ei.setEventtimestamp(approximateArrivalTimestamp);

        return ei;
    }

    @Override
    public TypeInformation<EmployeeInfo> getProducedType() {
        return TypeExtractor.getForClass(EmployeeInfo.class);
    }
}
