package poststats.kafkaConsumer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import poststats.datatypes.CommentEvent;
import poststats.datatypes.LikeEvent;
import poststats.datatypes.PostEvent;
import poststats.datatypes.StatEvent;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

public class Analytics1 {

    public static void main(String[] args) throws Exception {

        final long servingStartTime = Calendar.getInstance().getTimeInMillis();
        // Read from input parameters - time of first post in the simulation
        final long dataStartTime = Instant.parse("2012-02-02T02:46:56Z").toEpochMilli();
        final int maxEventDelaySecs = 60;
        final int speedUpFactor = 2500;

        ComputeStats statObj = new ComputeStats();

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<PostEvent> postEventDataStream = env.addSource(new EventReaderFromKafka<PostEvent>(
                "postStream",
                maxEventDelaySecs,
                speedUpFactor,
                PostEvent.class,
                servingStartTime,
                dataStartTime
        )).returns(poststats.datatypes.PostEvent.class);

        //postEventDataStream.print().setParallelism(1);

        DataStream<CommentEvent> commentEventDataStream = env.addSource(new EventReaderFromKafka<CommentEvent>(
                "commentStream",
                maxEventDelaySecs,
                speedUpFactor,
                CommentEvent.class,
                servingStartTime,
                dataStartTime
        )).returns(poststats.datatypes.CommentEvent.class);

        //commentEventDataStream.print().setParallelism(1);

        DataStream<LikeEvent> likeEventDataStream = env.addSource(new EventReaderFromKafka<LikeEvent>(
                "likeStream",
                maxEventDelaySecs,
                speedUpFactor,
                LikeEvent.class,
                servingStartTime,
                dataStartTime
        )).returns(poststats.datatypes.LikeEvent.class);

        //likeEventDataStream.print().setParallelism(1);

        /*
        DataStream<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> mappedPostStream =
                postEventDataStream
                        .map(new MapFunction<PostEvent, Tuple6<Long, Integer, Integer, Integer, Integer, Long>>() {
                            @Override
                            public Tuple6<Long, Integer, Integer, Integer, Integer, Long> map(PostEvent event) {
                                return new Tuple6<>(
                                        event.getPostId(), //PostID
                                        0, //1 if comment; commentCount
                                        0, //1 if reply; replyCount
                                        0, // likeCount from other stream
                                        event.getPersonId(), // person who caused this event
                                        event.getTimestamp() // time of event
                                );
                            }
                        });
        */

        DataStream<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> mappedCommentStream =
        commentEventDataStream
                .map(new MapFunction<CommentEvent, Tuple6<Long, Integer, Integer, Integer, Integer, Long>>() {
                    @Override
                    public Tuple6<Long, Integer, Integer, Integer, Integer, Long> map(CommentEvent event) {
                        return new Tuple6<>(
                                event.getPostId(), //PostID
                                (event.getReplyToCommentId()==-1)?1:0, //1 if comment; commentCount
                                (event.getReplyToCommentId()!=-1)?1:0, //1 if reply; replyCount
                                0, // likeCount from other stream
                                event.getPersonId(), // person who caused this event
                                event.getTimestamp() // time of event
                        );
                    }
                });

        DataStream<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> mappedLikeStream =
        likeEventDataStream
                .map(new MapFunction<LikeEvent, Tuple6<Long, Integer, Integer, Integer, Integer, Long>>() {
                    @Override
                    public Tuple6<Long, Integer, Integer, Integer, Integer, Long> map(LikeEvent event) {
                        return new Tuple6<>(
                                event.getPostId(), //PostID
                                0, //1 if comment; commentCount
                                0, //1 if reply; replyCount
                                1, // likeCount from other stream
                                event.getPersonId(), // person who caused this event
                                event.getTimestamp() // time of event
                        );
                    }
                });

        DataStream<StatEvent> result1 = mappedCommentStream.union(mappedLikeStream)
                .flatMap(statObj)
                .keyBy(0)
                .timeWindow(Time.minutes(30))
                .reduce(new EventReduceFunction(), new EventProcessWindowFunction());

        DataStream<String> kafkaDisplay = result1.timeWindowAll(Time.minutes(30))
                .process(new CombineProcessFunction())
                .map(new MapFunction<Tuple4<String, String, Integer, HashMap<Long, Tuple5<Integer, Integer, Integer, Long, String>>>, String>() {
                    @Override
                    public String map(Tuple4<String, String, Integer, HashMap<Long, Tuple5<Integer, Integer, Integer, Long, String>>> tuple) {
                        String output = "WindowStart: "+ tuple.f0 + " WindowEnd: "+ tuple.f1 + " #results: " +  tuple.f2 + "\n";

                        for (Map.Entry<Long, Tuple5<Integer, Integer, Integer, Long, String>> entry : tuple.f3.entrySet()) {
                            Long pId = entry.getKey();
                            Tuple5<Integer, Integer, Integer, Long, String> t = entry.getValue();
                            output = output + "PostID: " + pId + " | " + t.toString() +"\n";
                        }
                        return output;
                    }
                });


        // Write intermediate results to kafka
        FlinkKafkaProducer011<String> statProducer = new FlinkKafkaProducer011<String>(
                "localhost:9093", // broker list
                "resultStream1", // target topic
                new SimpleStringSchema()); // serialization schema

        kafkaDisplay.addSink(statProducer);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    // Function definitions

    // Keeps the updated record with the highest timestamp for that key
    private static class EventReduceFunction implements ReduceFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> {
        @Override
        public Tuple6<Long, Integer, Integer, Integer, Integer, Long> reduce(
                Tuple6<Long, Integer, Integer, Integer, Integer, Long> e1, Tuple6<Long, Integer, Integer, Integer, Integer, Long> e2) {
            if(e1.f5 <= e2.f5){
                return e2;
            }else{
                return e1;
            }
        }
    }

    // Converts the results into a formal class StatEvent (mainly for intermediate debug purpose)
    private static class EventProcessWindowFunction
            extends ProcessWindowFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Long>,
            StatEvent,
            Tuple,
            TimeWindow> {
        @Override
        public void process(Tuple key,
                            Context context,
                            Iterable<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> minReadings,
                            Collector<StatEvent> out) {
            Tuple6<Long, Integer, Integer, Integer, Integer, Long> min = minReadings.iterator().next();

            StatEvent s = new StatEvent(min.f0, min.f1, min.f2, min.f3, min.f4, min.f5,
                    context.window().getStart(), context.window().getEnd());

            out.collect(s);
        }
    }

    // We receive all results across all posts for a given window
    // Iterating over it, we apply the logic to retain only active post statistics

    private static class CombineProcessFunction extends
            ProcessAllWindowFunction<StatEvent, Tuple4<String, String, Integer, HashMap<Long, Tuple5<Integer, Integer, Integer, Long, String>>>, TimeWindow>
    {
        //Key: PostId
        //Value: (CommentCount, ReplyCount, UniqueUserCount, lastActivityTimestamp, stringTimestamp)
        private HashMap<Long, Tuple5<Integer, Integer, Integer, Long, String>> resultStats;

        @Override
        public void open(Configuration parameters) throws Exception{
            resultStats = new HashMap<>();
        }

        @Override
        public void process(Context context, Iterable<StatEvent> iterable,
                            Collector<Tuple4<String, String, Integer, HashMap<Long, Tuple5<Integer, Integer, Integer, Long, String>>>> collector) throws Exception {

            // Get window stats
            long windowStartTime = context.window().getStart();
            long windowEndTime = context.window().getEnd();

            // UniqueCount needs to be updated only once an hour
            // We check which window we are dealing with and output old or new uniqueCount
            // But the original result stats always has the right up-to-date value
            int endMinutes = getMinutesFromDateString(windowEndTime);
            boolean updateUniqueCount = (endMinutes!=30); //update only at the end of every hourly window

            // Filter active posts by comparing time stamp of previous results
            // Decide whether to persist them in this window too
            if(!resultStats.isEmpty()){
                List<Long> removeIds = new ArrayList<Long>();

                Iterator<Map.Entry<Long, Tuple5<Integer, Integer, Integer, Long, String>>> it =
                        resultStats.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry<Long, Tuple5<Integer, Integer, Integer, Long, String>> pair = it.next();
                    Long pId = pair.getKey();
                    Tuple5<Integer, Integer, Integer, Long, String> t = pair.getValue();
                    if(t.f3 < windowEndTime - 12*60*60*1000){
                        // System.out.println("Dropped: " + pId + " " + t.f3 + " " + (windowEndTime - 12*60*60*1000));
                        removeIds.add(pId);
                    }
                }

                // Remove all inactive posts
                resultStats.keySet().removeAll(removeIds);
            }

            // Create a temporary hashmap to output
            HashMap<Long, Tuple5<Integer, Integer, Integer, Long, String>> newStats = new HashMap<>(resultStats);

            for (StatEvent event : iterable) {
                Long postId = event.getPostId();
                Integer commentCount = event.getCommentCount();
                Integer replyCount = event.getReplyCount();
                Integer uniquePersonCount = event.getUniquePeopleCount();
                Long lastActivityTime = event.getLastActivityTimestamp();
                String activityTimeStr = event.getActivityTime();

                // Add stats to new stats considering window semantics
                if(updateUniqueCount){
                    newStats.put(postId, new Tuple5<>(commentCount,replyCount,uniquePersonCount,
                            lastActivityTime, activityTimeStr));
                }else{
                    Integer oldUniqueCount = (newStats.containsKey(postId))? newStats.get(postId).f2:0;
                    newStats.put(postId, new Tuple5<>(commentCount,replyCount,oldUniqueCount,
                            lastActivityTime, activityTimeStr));
                    //System.out.println("Added oldUC: "+ endMinutes + " " + oldUniqueCount);
                }

                // always update resultStats with the iterable event - OVERWRITE
                resultStats.put(postId, new Tuple5<>(commentCount,replyCount,uniquePersonCount,
                        lastActivityTime, activityTimeStr));
            }

            Integer numberOfResultsInWindow = newStats.size();
            collector.collect(new Tuple4<>(getDateString(context.window().getStart()),
                    getDateString(context.window().getEnd()),
                    numberOfResultsInWindow,
                    newStats));
        }

        public static String getDateString(long windowTime){

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date dt = new Date(windowTime);
            return sdf.format(dt);
        }

        public static Integer getMinutesFromDateString(long windowEndTime){

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date dt = new Date(windowEndTime);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(dt);
            return calendar.get(Calendar.MINUTE);
        }
    }

    // FlatMap function to output the updated statistics with every input event at its timestamp
    // Uses a HashMap as managed state
    public static final class ComputeStats
            extends RichFlatMapFunction<Tuple6<Long, Integer, Integer, Integer, Integer, Long>,
            Tuple6<Long, Integer, Integer, Integer, Integer, Long>>
    {

        private HashMap<Long, Tuple6<Long, Integer, Integer, Integer, List<Integer>, Long>> stats;

        @Override
        public void open(Configuration parameters) throws Exception{
            stats = new HashMap<>();
        }
        @Override
        public void flatMap(Tuple6<Long, Integer, Integer, Integer, Integer, Long> in,
                            Collector<Tuple6<Long, Integer, Integer, Integer, Integer, Long>> out) throws Exception{

            Long postId = in.f0;
            Integer commentCount = in.f1;
            Integer replyCount = in.f2;
            Integer likeCount = in.f3;
            Integer personId = in.f4;
            Long lastActivityTime = in.f5;

            if(stats.containsKey(postId)){
                // Update existing HashMap value
                Integer newCommentCount = stats.get(postId).f1 + commentCount;
                Integer newReplyCount = stats.get(postId).f2 + replyCount;
                Integer newLikeCount = stats.get(postId).f3 + likeCount;

                // Maintain list of uniquely engaged users
                List<Integer> newPersonList;
                if(stats.get(postId).f4.contains(personId)){
                    newPersonList = stats.get(postId).f4;
                }else{
                    stats.get(postId).f4.add(personId);
                    newPersonList = stats.get(postId).f4;
                }

                // Last activity time after comparision
                Long newLastActivityTime = (stats.get(postId).f5 < lastActivityTime)? lastActivityTime:
                        stats.get(postId).f5;

                stats.put(postId, new Tuple6<>(
                        postId,
                        newCommentCount,
                        newReplyCount,
                        newLikeCount,
                        newPersonList,
                        newLastActivityTime
                ));

            }else{
                List<Integer> newPersonList = new ArrayList<Integer>();
                newPersonList.add(personId);

                // Insert new user in HashMap
                Tuple6<Long, Integer, Integer, Integer, List<Integer>, Long> addRecord = new Tuple6<>(
                        postId,
                        commentCount,
                        replyCount,
                        likeCount,
                        newPersonList,
                        lastActivityTime
                );
                //System.out.println("New record: " + addRecord);
                stats.put(postId, addRecord);
            }

            /*
            System.out.println(
                    stats.get(postId).f0 + " " +
                    stats.get(postId).f1 + " " +
                    stats.get(postId).f2 + " " +
                    stats.get(postId).f3 + " " +
                    stats.get(postId).f4.size() + " " +
                    stats.get(postId).f5
            );*/

            out.collect(new Tuple6<Long, Integer, Integer, Integer, Integer, Long>(
                    stats.get(postId).f0,
                    stats.get(postId).f1,
                    stats.get(postId).f2,
                    stats.get(postId).f3,
                    stats.get(postId).f4.size(),
                    stats.get(postId).f5
                    ));
        }
    }
}

