package com.amazonaws.services.kinesisanalytics.sources;

import com.amazonaws.services.kinesisanalytics.payloads.ConfigSettings;
import com.github.javafaker.Faker;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;


public class MockConfigSettingsSource extends RichParallelSourceFunction<ConfigSettings> {

    private static final Logger LOG = LoggerFactory.getLogger(RandEmployeeInfoSource.class);

    private static final Long WATERMARK_GEN_PERIOD = 35L;
    private static final Long SLEEP_PERIOD = 2L;

    private transient Faker _faker;
    private transient SimpleDateFormat _sdfr;

    private volatile boolean isRunning = true;

    public MockConfigSettingsSource() {
        initIfNecessary();
    }

    @Override
    public void run(SourceContext<ConfigSettings> sourceContext) throws Exception {
        Long lastTokenGenTime = System.currentTimeMillis();
        ConfigSettings cs = getNextSetting();

        while(isRunning) {
            Long currTime = System.currentTimeMillis();

            if((currTime - lastTokenGenTime) > 3000L) {
                cs = getNextSetting();
                lastTokenGenTime = System.currentTimeMillis();
            }

            sourceContext.collectWithTimestamp(cs, cs.getTokenIssueTimestamp());

            // for demo only
            if(currTime % WATERMARK_GEN_PERIOD == 0) {
                sourceContext.emitWatermark(new Watermark(currTime));
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

    private ConfigSettings getNextSetting() {
        initIfNecessary();

        ConfigSettings cs = new ConfigSettings();

        cs.setTokenIssueTimestamp(System.currentTimeMillis());
        cs.setThreshold(ThreadLocalRandom.current().nextInt(49));
        cs.setSampleCount(ThreadLocalRandom.current().nextInt(10));

        return cs;
    }
}