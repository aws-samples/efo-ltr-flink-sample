package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class JsonToEmployeeInfoMap extends RichMapFunction<String, EmployeeInfo> {
    ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        objectMapper = new ObjectMapper();
    }

    @Override
    public EmployeeInfo map(String s) throws Exception {
        return objectMapper.readValue(s, EmployeeInfo.class);
    }
}