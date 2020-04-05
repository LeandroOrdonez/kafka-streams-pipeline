package querying.util;

import model.TemperatureReading;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

// Extracts the embedded timestamp of a record (giving you "event time" semantics).
public class TSExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        TemperatureReading reading = (TemperatureReading) record.value();
        if (reading != null) {
            long timestamp = reading.getTimestamp();
            if (timestamp < 0) {
                throw new RuntimeException("Invalid negative timestamp.");
            }
            return timestamp;
        } else {
            return record.timestamp();
        }
    }
}