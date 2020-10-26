/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.payloads.EmployeeInfo;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.amazonaws.services.kinesisanalytics.utils.EmployeeInfoBucketAssigner;
import com.amazonaws.services.kinesisanalytics.utils.EmployeeInfoDeserializationSchema;
import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.connectors.flink.FlinkKinesisConsumer;
import software.amazon.kinesis.connectors.flink.config.AWSConfigConstants;
import software.amazon.kinesis.connectors.flink.config.ConsumerConfigConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private static final String DEFAULT_STREAM_NAME = "KDAScalingTestStream";
    private static final String DEFAULT_REGION_NAME = "us-east-2";
    private static final Long TIME_SLICE = 60L * 60L; // 1 hour

    public static DataStream<EmployeeInfo> createKinesisEFOSource(StreamExecutionEnvironment env,
                                                            ParameterTool parameter,
                                                            String efoConsumerName,
                                                            long initialTimestamp) throws Exception {
        Properties consumerConfig = new Properties();

        // Need to specify the initial position as a timestamp
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "AT_TIMESTAMP");
        consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, String.valueOf(initialTimestamp));

        consumerConfig.put(AWSConfigConstants.AWS_REGION, DEFAULT_REGION_NAME);
        consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
                ConsumerConfigConstants.RecordPublisherType.EFO.name());
        consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, efoConsumerName);

        consumerConfig.put(ConsumerConfigConstants.EFO_REGISTRATION_TYPE,
                ConsumerConfigConstants.EFORegistrationType.EAGER.name());

        String stream = parameter.get("InputStreamName", DEFAULT_STREAM_NAME);

        FlinkKinesisConsumer<EmployeeInfo> flinkKinesisConsumer =
                new FlinkKinesisConsumer<>(stream, new EmployeeInfoDeserializationSchema(), consumerConfig);

        DataStream<EmployeeInfo> kinesis = env.addSource(flinkKinesisConsumer);

        return kinesis;
    }

    private static DataStream<EmployeeInfo> setupCatchupProcessingEFOConsumer(StreamExecutionEnvironment env,
                                                                              ParameterTool parameter,
                                                                              String efoConsumerName,
                                                                              String mapStateDescriptorName,
                                                                              long beginTimestamp,
                                                                              long endTimestamp) throws Exception {
        // set up threshold so the filter operator below will filter out records
        // greater than or equal to threshold
        DataStream<Long> inputThresholdStream = env.fromElements(endTimestamp);

        MapStateDescriptor<Void, Long> bcStateDescriptor =
                new MapStateDescriptor<>(mapStateDescriptorName, Types.VOID, Types.LONG);

        BroadcastStream<Long> broadcastTokens =
                inputThresholdStream.broadcast(bcStateDescriptor);

        DataStream<EmployeeInfo> input =
                createKinesisEFOSource(env, parameter, efoConsumerName, beginTimestamp)
                        .connect(broadcastTokens)
                        .process(new TimeSliceFilter());

        return input;
    }

    private static SinkFunction<EmployeeInfo> getS3Sink(ParameterTool parameter) {

        String bucket = parameter.get("OutputBucket", "s3://ktohio/kdascalingtest1/");
        String prefix = String.format("%sjob_start=%s/", parameter.get("OutputPrefix", ""),
                new org.joda.time.DateTime().toString("yyyyMMddHHmmss"));

        return StreamingFileSink
                .forBulkFormat(
                        new Path(bucket),
                        ParquetAvroWriters.forReflectRecord(EmployeeInfo.class)
                )
                .withBucketAssigner(new EmployeeInfoBucketAssigner(prefix))
                .build();
    }

    private static DataStream<EmployeeInfo> setupForCatchup(StreamExecutionEnvironment env,
                                                            ParameterTool parameter) throws Exception {
        // we'll register the requisite number of EFO consumers
        // For instance, with 5 consumers, we'll be able to increase our
        // max consumption capacity to 5 * 2MB/s = 10MB/s
        long endTimestamp = parameter.getLong("catchupProcessingEndTimestamp");

        DataStream<EmployeeInfo> efoInput1 = setupCatchupProcessingEFOConsumer(env,
                parameter,
                "my-efo-consumer-1",
                "thresholdBroadcastState1",
                endTimestamp - TIME_SLICE,
                endTimestamp);

        endTimestamp = endTimestamp - TIME_SLICE;
        DataStream<EmployeeInfo> efoInput2 = setupCatchupProcessingEFOConsumer(env,
                parameter,
                "my-efo-consumer-2",
                "thresholdBroadcastState2",
                endTimestamp - TIME_SLICE,
                endTimestamp);

        endTimestamp = endTimestamp - TIME_SLICE;
        DataStream<EmployeeInfo> efoInput3 = setupCatchupProcessingEFOConsumer(env,
                parameter,
                "my-efo-consumer-3",
                "thresholdBroadcastState3",
                endTimestamp - TIME_SLICE,
                endTimestamp);

        endTimestamp = endTimestamp - TIME_SLICE;
        DataStream<EmployeeInfo> efoInput4 = setupCatchupProcessingEFOConsumer(env,
                parameter,
                "my-efo-consumer-4",
                "thresholdBroadcastState4",
                endTimestamp - TIME_SLICE,
                endTimestamp);

        endTimestamp = endTimestamp - TIME_SLICE;
        DataStream<EmployeeInfo> efoInput5 = setupCatchupProcessingEFOConsumer(env,
                parameter,
                "my-efo-consumer-5",
                "thresholdBroadcastState5",
                endTimestamp - TIME_SLICE,
                endTimestamp);

        // Let's union all streams so we can work with just 1 stream
        DataStream<EmployeeInfo> eiSTream =
                efoInput1.union(efoInput2).union(efoInput3).union(efoInput4).union(efoInput5);

        return eiSTream;
    }

    private static DataStream<EmployeeInfo> setupForNormal(StreamExecutionEnvironment env,
                                                           ParameterTool parameter) throws Exception {
        long beginTimestamp = parameter.getLong("normalProcessingBeginTimestamp");
        return createKinesisEFOSource(env, parameter, "my-efo-consumer-1", beginTimestamp);
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use approx arrival time within Kinesis Data Streams for timestamp
        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kinesis.html#event-time-for-consumed-records
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ParameterTool parameter;

        if (env instanceof LocalStreamEnvironment) {
            //read the parameters specified from the command line
            parameter = ParameterTool.fromArgs(args);
        } else {
            //read the parameters from the Kinesis Analytics environment
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

            Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

            if (flinkProperties == null) {
                throw new RuntimeException("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime.");
            }

            parameter = ParameterToolUtils.fromApplicationProperties(flinkProperties);
        }

        DataStream<EmployeeInfo> eiSTream;

        if (parameter.get("processingMode", "normal") == "catchup") {
            eiSTream = setupForCatchup(env, parameter);
        } else {
            eiSTream = setupForNormal(env, parameter);
        }

        // This is a sample operator. In your use case, you could replace this with a
        // call to an external service or some other kind of operator
        DataStream<EmployeeInfo> postOpStream = eiSTream.flatMap(new MyOperator());

        postOpStream.addSink(getS3Sink(parameter)).name("S3Sink");

        // execute program
        env.execute("FlinkKinesisConsumer w/ EFO sample");
    }
}