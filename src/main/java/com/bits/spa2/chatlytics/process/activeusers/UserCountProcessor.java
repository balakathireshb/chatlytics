package com.bits.spa2.chatlytics.process.activeusers;

import java.time.Duration;
import java.util.ArrayList;
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
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

@Configuration
public class UserCountProcessor {

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

    private static final String storeName = "userEventId-store";

    public Properties getStremConfig() {
        Properties configurations = new Properties();
        configurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        configurations.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        configurations.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointAlgConfig);
        configurations.put("sasl.mechanism", saslMechanism);
        configurations.put("sasl.jaas.config", saslJaasConfig);

        configurations.put(StreamsConfig.APPLICATION_ID_CONFIG, UserCountProcessor.class.getSimpleName());

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
        // How long we "remember" an event. During this time, any incoming duplicates of
        // the event
        // will be, well, dropped, thereby de-duplicating the input data.
        //
        // The actual value depends on your use case. To reduce memory and disk usage,
        // you could
        // decrease the size to purge old windows more frequently at the cost of
        // potentially missing out
        // on de-duplicating late-arriving records.
        final Duration windowSize = Duration.ofMinutes(1);

        // retention period must be at least window size -- for this use case, we don't
        // need a longer retention period
        // and thus just use the window size as retention time
        final Duration retentionPeriod = windowSize;
        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(storeName, retentionPeriod, windowSize, false), Serdes.String(),
                Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);

        KStream<String, Message> msgStream = builder.stream("chat-topic", Consumed.with(Serdes.String(), messageSerde));

        KStream<String, Message> distinctSenderStream = msgStream.transformValues(
                () -> new DeduplicationTransformer<>(windowSize.toMillis(), (key, msg) -> msg.getSender()), storeName)
                .filter((k, v) -> v != null);

        KStream<String, Integer> distinctSenderStreamWithKey = distinctSenderStream.flatMap((key, value) -> {
            List<KeyValue<String, Integer>> result = new LinkedList<>();
            result.add(KeyValue.pair("user", 1));
            return result;
        });
        TimeWindows tw = TimeWindows.of(Duration.ofSeconds(60L)).advanceBy(Duration.ofSeconds(10L));
        KGroupedStream<String, Integer> distinctSenderStreamGrouped = distinctSenderStreamWithKey
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()));
        distinctSenderStreamGrouped.windowedBy(tw).count().toStream()
                .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(),
                        String.valueOf(value)))
                .to(Constants.KAFKA_ACTIVE_USERS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    /**
     * Discards duplicate click events from the input stream by ip address
     * <p>
     * Duplicate records are detected based on ip address The transformer remembers
     * known ip addresses within an associated window state store, which
     * automatically purges/expires IPs from the store after a certain amount of
     * time has passed to prevent the store from growing indefinitely.
     * <p>
     * Note: This code is for demonstration purposes and was not tested for
     * production usage.
     */
    private static class DeduplicationTransformer<K, V, E> implements ValueTransformerWithKey<K, V, V> {

        private ProcessorContext context;

        /**
         * Key: ip address Value: timestamp (event-time) of the corresponding event when
         * the event ID was seen for the first time
         */
        private WindowStore<E, Long> eventIdStore;

        private final long leftDurationMs;
        private final long rightDurationMs;

        private final KeyValueMapper<K, V, E> idExtractor;

        /**
         * @param maintainDurationPerEventInMs how long to "remember" a known ip address
         *                                     during the time of which any incoming
         *                                     duplicates will be dropped, thereby
         *                                     de-duplicating the input.
         * @param idExtractor                  extracts a unique identifier from a
         *                                     record by which we de-duplicate input
         *                                     records; if it returns null, the record
         *                                     will not be considered for de-duping but
         *                                     forwarded as-is.
         */
        DeduplicationTransformer(final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
            if (maintainDurationPerEventInMs < 1) {
                throw new IllegalArgumentException("maintain duration per event must be >= 1");
            }
            leftDurationMs = maintainDurationPerEventInMs / 2;
            rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
            this.idExtractor = idExtractor;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            this.context = context;
            eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
        }

        @Override
        public V transform(final K key, final V value) {
            final E eventId = idExtractor.apply(key, value);
            if (eventId == null) {
                return value;
            } else {
                final V output;
                if (isDuplicate(eventId)) {
                    output = null;
                    updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
                } else {
                    output = value;
                    rememberNewEvent(eventId, context.timestamp());
                }
                return output;
            }
        }

        private boolean isDuplicate(final E eventId) {
            final long eventTime = context.timestamp();
            final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(eventId, eventTime - leftDurationMs,
                    eventTime + rightDurationMs);
            final boolean isDuplicate = timeIterator.hasNext();
            timeIterator.close();
            return isDuplicate;
        }

        private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, final long newTimestamp) {
            eventIdStore.put(eventId, newTimestamp, newTimestamp);
        }

        private void rememberNewEvent(final E eventId, final long timestamp) {
            eventIdStore.put(eventId, timestamp, timestamp);
        }

        @Override
        public void close() {
            // Note: The store should NOT be closed manually here via
            // `eventIdStore.close()`!
            // The Kafka Streams API will automatically close stores when necessary.
        }

    }

    public void process() {
        KafkaStreams streams = new KafkaStreams(buildTopology(), getStremConfig());
        streams.start();

    }

}
