package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.operators.HashingKafkaPartitioner;
import com.cloudera.streaming.examples.flink.utils.Utils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Optional;

public class SimpleKafkaNumberProcessorJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Path to the properties file is expected as the only argument.");
        }
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);
        new SimpleKafkaNumberProcessorJob()
                .createApplicationPipeline(params)
                .execute("Simple Kafka Number Processor Job");
    }

    public final StreamExecutionEnvironment createApplicationPipeline(ParameterTool params) throws Exception {

        // Create and configure the StreamExecutionEnvironment
        StreamExecutionEnvironment env = createExecutionEnvironment(params);

        // Read number stream
        DataStream<String> numberStream = readNumberStream(params, env);

        // Handle the output of transaction and query results separately
        writeOutput(params, numberStream);

        return env;
    }

    private StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // We set max parallelism to a number with a lot of divisors
        env.setMaxParallelism(360);

        int parallelism = params.getInt("parallelism", 0);
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }

        return env;
    }

    public DataStream<String> readNumberStream(ParameterTool params, StreamExecutionEnvironment env) {
        FlinkKafkaConsumer<String> numberSource = new FlinkKafkaConsumer<>(
                SimpleKafkaNumberGeneratorJob.NUMBER_INPUT_TOPIC, new SimpleStringSchema(),
                Utils.readKafkaProperties(params, true));

        numberSource.setCommitOffsetsOnCheckpoints(true);
        numberSource.setStartFromLatest();

        return env.addSource(numberSource)
                .name("Kafka Random Number Source")
                .uid("Kafka Random Number Source");
    }

    public void writeOutput(ParameterTool params, DataStream<String> numberStream) {
        FlinkKafkaProducer<String> outputSink = new FlinkKafkaProducer<>(
                "number.output.topic", new SimpleStringSchema(),
                Utils.readKafkaProperties(params, false),
                Optional.of(new HashingKafkaPartitioner<>()));

        numberStream
                .addSink(outputSink)
                .name("Kafka Number Result Sink")
                .uid("Kafka Number Result Sink");
    }
}
