package com.amazonaws.services.kinesisanalytics.utils;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.joda.time.DateTime;

import java.io.Serializable;

public class EmployeeInfoBucketAssigner implements BucketAssigner<EmployeeInfo, String>, Serializable {
    private final String prefix;

    public EmployeeInfoBucketAssigner(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String getBucketId(EmployeeInfo employeeInfo, Context context) {

        return String.format("%scbucket=%04d/dobyear=%04d",
                prefix,
                employeeInfo.getCompanyid() % 10,
                DateTime.parse(employeeInfo.getDob().toString()).getYear()
        );
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}