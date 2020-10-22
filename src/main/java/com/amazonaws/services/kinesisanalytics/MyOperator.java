package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class MyOperator extends RichFlatMapFunction<EmployeeInfo, EmployeeInfo> {

    @Override
    public void flatMap(EmployeeInfo employeeInfo, Collector<EmployeeInfo> collector) throws Exception {
        if(employeeInfo.getEmployeeid() > 3) {
            collector.collect(employeeInfo);
        }
    }
}