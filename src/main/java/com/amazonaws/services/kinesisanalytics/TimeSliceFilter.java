package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class TimeSliceFilter extends BroadcastProcessFunction<EmployeeInfo, Long, EmployeeInfo> {
    private static Long THRESHOLD = System.currentTimeMillis()/1000;

    private transient ListState<EmployeeInfo> bufferedElements;

    // broadcast state descriptor
    MapStateDescriptor<Void, Long> timestampThresholdDesc;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        timestampThresholdDesc =
                new MapStateDescriptor<>("timestampThreshold", Types.VOID, Types.LONG);

        ListStateDescriptor<EmployeeInfo> eiStateDescriptor =
                new ListStateDescriptor<>(
                        "bufferedElements",
                        Types.POJO(EmployeeInfo.class));

        bufferedElements = getRuntimeContext().getListState(eiStateDescriptor);
    }

    @Override
    public void processElement(EmployeeInfo employeeInfo,
                               ReadOnlyContext readOnlyContext,
                               Collector<EmployeeInfo> collector) throws Exception {
        // get current config settings from broadcast state
        Long threshold = readOnlyContext
                .getBroadcastState(this.timestampThresholdDesc)
                // access MapState with null as VOID default value
                .get(null);

        if(threshold == null) {
            // buffer till we get a threshold to compare against and filter
            bufferedElements.add(employeeInfo);
        } else {
            // first deal w/ buffer
            Iterable<EmployeeInfo> bufferedIterables = bufferedElements.get();
            if(bufferedIterables != null) {
                for (EmployeeInfo ei : bufferedIterables) {
                    if (ei.getEventtimestamp() < threshold) {
                        collector.collect(ei);
                    }
                }

                bufferedElements.clear();
            }

            // now deal w/ current message
            if (employeeInfo.getEventtimestamp() < threshold) {
                collector.collect(employeeInfo);
            }
        }
    }

    @Override
    public void processBroadcastElement(Long aLong,
                                        Context context,
                                        Collector<EmployeeInfo> collector) throws Exception {
        // store the threshold value by updating the broadcast state
        BroadcastState<Void, Long> bcState = context.getBroadcastState(this.timestampThresholdDesc);
        // storing in MapState with null as VOID default value
        bcState.put(null, aLong);
    }
}
