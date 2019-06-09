/*
 * Copyright 2015 data Artisans GmbH
 */

package poststats.kafkaConsumer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import poststats.datatypes.CommonEvent;
import java.util.*;

/**
 * The following approach for Event replay is based on the open source project called flink training examples
 * by Vervica on GitHub (https://github.com/ververica/flink-training-exercises(Copyright 2015 data Artisans GmbH)).
 * The code has been adapted to serve our use-case where we poll events from Kafka.
 *
 * This SourceFunction generates a data stream of T Event records which are read from a given Kafka topic.
 * Each record has a time stamp and the input file must be ordered by this time stamp.
 *
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served slightly out-of-order of their timestamps.
 *
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 *
 */
public class EventReaderFromKafka<T> implements SourceFunction<T> {

    private Class<T> typeParameterClass;

    private final long servingStartTime;
    private final long dataStartTime;

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String topic;
    private final int servingSpeed;

    public EventReaderFromKafka(String topic, int maxEventDelaySecs, int servingSpeedFactor,
                                Class<T> typeParameterClass, long servingStartTime, long dataStartTime) {
        if(maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.topic = topic;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
        this.typeParameterClass = typeParameterClass;
        this.servingStartTime = servingStartTime;
        this.dataStartTime = dataStartTime;

        System.out.println("Event reader constructor for "+ this.typeParameterClass);
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        generateUnorderedStream(sourceContext);
    }

    private void generateUnorderedStream(SourceContext<T> sourceContext) throws Exception {

        Random rand = new Random(6666);

        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                100,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });

        // Poll events using kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", "0");

        // create a Kafka consumer
        KafkaConsumer<Long, T> eventConsumer = new KafkaConsumer<Long, T>(kafkaProps,
                new LongDeserializer(),
                new EventDeserializer<>(this.typeParameterClass)
        );

        eventConsumer.subscribe(Collections.singletonList(this.topic));

        boolean firstEvent = true;

        try {
            while (true) {
                ConsumerRecords<Long, T> records = eventConsumer.poll(0);
                Iterator<ConsumerRecord<Long, T>> iterator = records.iterator();

                if(iterator.hasNext())
                {
                    T event = iterator.next().value();

                    // read events one-by-one and emit a random event from the buffer each time
                    while (emitSchedule.size() > 0 || firstEvent) {

                        // insert all events into schedule that might be emitted next
                        long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
                        long eventEventTime = event != null ? ((CommonEvent)event).getTimestamp() : -1;

                        while(event != null && ( // while there is a event AND
                                emitSchedule.isEmpty() || // and no event in schedule OR
                                        eventEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough events in schedule
                        )
                        {
                            // insert event into emit schedule
                            long delayedEventTime = eventEventTime + getNormalDelayMsecs(rand);
                            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, event));

                            if(firstEvent){
                                // Need to add first watermark to emit schedule explicitly
                                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                                emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
                                firstEvent = false;
                            }

                            // read next event
                            if (iterator.hasNext()) {
                                event = iterator.next().value();
                                eventEventTime = ((CommonEvent)event).getTimestamp();
                            }
                            else {
                                event = null;
                                eventEventTime = -1;
                            }
                        }

                        // emit schedule is updated, emit next element in schedule
                        Tuple2<Long, Object> head = emitSchedule.poll();
                        long delayedEventTime = head.f0;

                        long now = Calendar.getInstance().getTimeInMillis();
                        long servingTime = toServingTime(delayedEventTime);
                        long waitTime = servingTime - now;

                        Thread.sleep( (waitTime > 0) ? waitTime : 0);

                        if(head.f1.getClass() == typeParameterClass) {
                            T emitevent = (T)(head.f1);
                            // emit event
                            // System.out.println("EMIT " + emitevent.toString());
                            sourceContext.collectWithTimestamp(emitevent, ((CommonEvent)emitevent).getTimestamp());
                        }
                        else if(head.f1 instanceof Watermark) {
                            Watermark emitWatermark = (Watermark)head.f1;
                            // emit watermark
                            sourceContext.emitWatermark(emitWatermark);
                            // schedule next watermark
                            long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
                        }
                    }
                }
            }
        }catch (Exception e){
            System.out.println("Error in Source : " + typeParameterClass.toString());
        }
        finally {
            eventConsumer.close();
        }

    }

    private long toServingTime(long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        // System.out.println("DIFF : "+ dataDiff/(1000*60) + " min");
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    private long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {

    }

}

