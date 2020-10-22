package com.amazonaws.services.kinesisanalytics.sources;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.github.javafaker.Faker;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

public class RandEmployeeInfoSource extends RichParallelSourceFunction<EmployeeInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(RandEmployeeInfoSource.class);

    private static final Long WATERMARK_GEN_PERIOD = 35L;
    private static final Long SLEEP_PERIOD = 2L;

    private transient Faker _faker;
    private transient SimpleDateFormat _sdfr;

    private volatile boolean isRunning = true;

    public RandEmployeeInfoSource() {
        initIfNecessary();
    }

    @Override
    public void run(SourceContext<EmployeeInfo> sourceContext) throws Exception {
        while(isRunning) {
            EmployeeInfo ei = getNextEmployee();

            sourceContext.collectWithTimestamp(ei, ei.getEventtimestamp());

            if(ei.getEventtimestamp() % WATERMARK_GEN_PERIOD == 0) {
                sourceContext.emitWatermark(new Watermark(ei.getEventtimestamp()));
            }

            Thread.sleep(SLEEP_PERIOD);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    private void initIfNecessary() {
        if(_faker == null) _faker = new Faker();
        if(_sdfr == null) _sdfr = new SimpleDateFormat("yyyy-MM-dd");
    }

    private EmployeeInfo getNextEmployee() {
        initIfNecessary();

        EmployeeInfo ei = new EmployeeInfo();

        ei.setCompanyid(ThreadLocalRandom.current().nextLong(5));
        ei.setEmployeeid(ThreadLocalRandom.current().nextLong(50));
        ei.setName(_faker.name().fullName());
        ei.setDob(_sdfr.format(_faker.date().birthday()));
        ei.setEventtimestamp(System.currentTimeMillis());

        return ei;
    }
}