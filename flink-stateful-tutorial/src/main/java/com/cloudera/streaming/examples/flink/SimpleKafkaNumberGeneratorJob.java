package com.cloudera.streaming.examples.flink;

import com.cloudera.streaming.examples.flink.operators.RandomNumberGenreatorSource;
import com.cloudera.streaming.examples.flink.types.SimpleIntegerSchema;
import com.cloudera.streaming.examples.flink.utils.Utils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class SimpleKafkaNumberGeneratorJob {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaNumberGeneratorJob.class);
    public static final String NUMBER_INPUT_TOPIC = "number.input.topic";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Path to the properties file is expected as the only argument.");
        }
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> generatedInput =
                env.addSource(new RandomNumberGenreatorSource(params))
                        .name("Random Number Generator");

        FlinkKafkaProducer<Integer> kafkaSink = new FlinkKafkaProducer<>(
                NUMBER_INPUT_TOPIC,
                new SimpleIntegerSchema(),
                Utils.readKafkaProperties(params, false),
                Optional.empty());

        generatedInput.addSink(kafkaSink).name("Random Number Kafka Sink");
        env.execute("Kafka Random Number Generator");
    }
}
