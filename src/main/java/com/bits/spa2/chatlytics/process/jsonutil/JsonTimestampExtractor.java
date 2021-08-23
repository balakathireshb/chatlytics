package com.bits.spa2.chatlytics.process.jsonutil;

import com.bits.spa2.chatlytics.model.Message;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * A timestamp extractor implementation that tries to extract event time from
 * the "timestamp" field in the Json formatted message.
 */
public class JsonTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
        if (record.value() instanceof Message) {
            return ((Message) record.value()).getTimestamp();
        }

        throw new IllegalArgumentException(
                "JsonTimestampExtractor cannot recognize the record value " + record.value());
    }
}