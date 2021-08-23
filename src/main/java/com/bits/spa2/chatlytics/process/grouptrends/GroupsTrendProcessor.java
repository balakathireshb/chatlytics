package com.bits.spa2.chatlytics.process.grouptrends;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.bits.spa2.chatlytics.Constants;
import com.bits.spa2.chatlytics.model.Message;
import com.bits.spa2.chatlytics.process.jsonutil.JsonPOJODeserializer;
import com.bits.spa2.chatlytics.process.jsonutil.JsonPOJOSerializer;
import com.bits.spa2.chatlytics.process.jsonutil.JsonTimestampExtractor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.yaml.snakeyaml.scanner.Constant;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;

@Configuration
public class GroupsTrendProcessor {

    @Value(value = "${kafka.properties.bootstrap.servers}")
    private String bootstrapAddress;

    @Value(value = "${kafka.properties.security.protocol}")
    private Object securityProtocol;

    @Value(value = "${kafka.properties.ssl.endpoint.identification.algorithm}")
    private Object sslEndpointAlgConfig;

    @Value(value = "${kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value(value = "${kafka.properties.sasl.jaas.config}")
    private Object saslJaasConfig;

    public Properties getStremConfig() {
        Properties configurations = new Properties();
        configurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        configurations.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        configurations.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointAlgConfig);
        configurations.put("sasl.mechanism", saslMechanism);
        configurations.put("sasl.jaas.config", saslJaasConfig);

        configurations.put(StreamsConfig.APPLICATION_ID_CONFIG, GroupsTrendProcessor.class.getSimpleName());

        configurations.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configurations.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        configurations.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        configurations.put(StreamsConfig.STATE_DIR_CONFIG, "C:/kafka/tmp");

        configurations.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        return configurations;
    }

    public Topology buildTopology() {
        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Message> messageSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Message.class);
        messageSerializer.configure(serdeProps, false);

        final Deserializer<Message> messageDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Message.class);
        messageDeserializer.configure(serdeProps, false);

        final Serde<Message> messageSerde = Serdes.serdeFrom(messageSerializer, messageDeserializer);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Message> msgStream = builder.stream(Constants.KAFKA_MESSAGE_TOPIC,
                Consumed.with(Serdes.String(), messageSerde));
        KStream<String, Integer> groupStream = msgStream.flatMap((key, value) -> {
            List<KeyValue<String, Integer>> result = new LinkedList<>();
            Message msg = (Message) value;
            String[] groups = msg.getTopics();
            for (String group : groups) {
                result.add(KeyValue.pair(group, 1));
            }
            return result;
        });

        KGroupedStream<String, Integer> msgGroupedByGroupsStream = groupStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));

        msgGroupedByGroupsStream.windowedBy(TimeWindows.of(Duration.ofSeconds(10L))).count().toStream()
                .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(),
                        String.valueOf(value)))
                .peek((key, value) -> System.out.println(key + "->" + value))
                .to(Constants.KAFKA_GROUP_TRENDS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public void process() {
        KafkaStreams streams = new KafkaStreams(buildTopology(), getStremConfig());
        streams.start();

    }

}
