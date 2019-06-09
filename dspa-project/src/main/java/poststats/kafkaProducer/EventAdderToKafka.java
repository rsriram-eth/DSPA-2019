/*
Class to uses custom sources to sink csv data into kafka from all 3 event types
Timestamp is ignored for now.
It is added after consuming and output with bounded delay.
 */

package poststats.kafkaProducer;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import poststats.datatypes.CommentEvent;
import poststats.datatypes.LikeEvent;
import poststats.datatypes.PostEvent;

public class EventAdderToKafka {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // execution plan
        DataStream<PostEvent> postEventDataStream = env.addSource(new PostAdder());

        DataStream<CommentEvent> commentEventDataStream = env.addSource(new CommentAdder());

        DataStream<LikeEvent> likeEventDataStream = env.addSource(new LikeAdder());

        // postEventDataStream.print();

        FlinkKafkaProducer011<PostEvent> postProducer = new FlinkKafkaProducer011<PostEvent>(
                "localhost:9092", // broker list
                "postStream", // target topic
                new EventSerializer<>(PostEvent.class)); // serialization schema

        postEventDataStream.addSink(postProducer);

        FlinkKafkaProducer011<CommentEvent> commentProducer = new FlinkKafkaProducer011<CommentEvent>(
                "localhost:9092", // broker list
                "commentStream", // target topic
                new EventSerializer<>(CommentEvent.class)); // serialization schema

        commentEventDataStream.addSink(commentProducer);

        FlinkKafkaProducer011<LikeEvent> likeProducer = new FlinkKafkaProducer011<LikeEvent>(
                "localhost:9092", // broker list
                "likeStream", // target topic
                new EventSerializer<>(LikeEvent.class)); // serialization schema

        likeEventDataStream.addSink(likeProducer);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
