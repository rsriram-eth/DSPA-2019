package poststats.kafkaConsumer;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.flink.api.common.functions.MapFunction;
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
import poststats.datatypes.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

public class Analytics2 {

    public static void main(String[] args) throws Exception {

        final long servingStartTime = Calendar.getInstance().getTimeInMillis();
        // Read from input parameters - time of first post in the simulation
        final long dataStartTime = Instant.parse("2012-02-02T02:46:56Z").toEpochMilli();
        final int maxEventDelaySecs = 60;
        final int speedUpFactor = 2500;

        ComputeInteractions recObj = new ComputeInteractions();

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        System.out.println(env.getParallelism());
        env.setParallelism(1);
        System.out.println(env.getParallelism());

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

        DataStream<Tuple5<Long, Long, Integer, Character, Long>> mappedPostStream2 =
                postEventDataStream
                        .map(new MapFunction<PostEvent, Tuple5<Long, Long, Integer, Character, Long>>() {
                            @Override
                            public Tuple5<Long, Long, Integer, Character, Long> map(PostEvent event) {
                                return new Tuple5<>(
                                        event.getPostId(), // MainEventId
                                        (long)-1, // ResponseToEventId
                                        event.getPersonId(), // person who caused this event
                                        event.getEventType(), // type of event
                                        event.getTimestamp()
                                );
                            }
                        });

        DataStream<Tuple5<Long, Long, Integer, Character, Long>> mappedCommentStream2 =
                commentEventDataStream
                        .map(new MapFunction<CommentEvent, Tuple5<Long, Long, Integer, Character, Long>>() {
                            @Override
                            public Tuple5<Long, Long, Integer, Character, Long> map(CommentEvent event) {
                                return new Tuple5<>(
                                        event.getCommentId(), //Actual CommentId/ReplyId
                                        (event.getReplyToCommentId()==-1)? event.getPostId():
                                                event.getReplyToCommentId(), //Response to which PostID/ CommentID(if reply)
                                        event.getPersonId(), // person who caused this event
                                        event.getEventType(), // type of event
                                        event.getTimestamp()
                                );
                            }
                        });

        DataStream<Tuple5<Long, Long, Integer, Character, Long>> mappedLikeStream2 =
                likeEventDataStream
                        .map(new MapFunction<LikeEvent, Tuple5<Long, Long, Integer, Character, Long>>() {
                            @Override
                            public Tuple5<Long, Long, Integer, Character, Long> map(LikeEvent event) {
                                return new Tuple5<>(
                                        (long)-1, // Like has no sub-events: so no Id
                                        event.getPostId(), // Like to which postId
                                        event.getPersonId(), // person who caused this event
                                        event.getEventType(), // type of event
                                        event.getTimestamp()
                                );
                            }
                        });

        DataStream<Tuple3<Integer, Integer, Long>> result2 =
        mappedPostStream2.union(mappedCommentStream2, mappedLikeStream2)
                .flatMap(recObj)
                .keyBy(0)
                .timeWindow(Time.hours(1))
                .process(new ProcessWindowFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>,
                        Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple3<Integer, Integer, Long>> iterable,
                                        Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {
                        for(Tuple3<Integer, Integer, Long> t: iterable){
                            collector.collect(t);
                        }
                    }
                });

        DataStream<String> kafkaDisplay = result2
                .timeWindowAll(Time.hours(1))
                .process(new GetRecommendationsProcessFunction())
                .map(new MapFunction<Tuple3<String, String, HashMap<Integer, List<SimilarityScore>>>, String>() {
                    @Override
                    public String map(Tuple3<String, String, HashMap<Integer, List<SimilarityScore>>> tuple) throws Exception {
                        String output = "\nWindowStart: "+ tuple.f0 + " WindowEnd: "+ tuple.f1 + "\n";

                        for (Map.Entry<Integer, List<SimilarityScore>> entry : tuple.f2.entrySet()) {
                            Integer personId = entry.getKey();
                            List<SimilarityScore> recommendations = entry.getValue();
                            for (SimilarityScore s: recommendations){
                                output = output + s.toString() + "\n";
                            }
                            output = output + "\n";
                        }

                        //System.out.println(output);
                        return output;
                    }
                });


        // Write intermediate results to kafka
        FlinkKafkaProducer011<String> recommendationsProducer = new FlinkKafkaProducer011<String>(
                "localhost:9093", // broker list
                "resultStream2", // target topic
                new SimpleStringSchema()); // serialization schema

        kafkaDisplay.addSink(recommendationsProducer);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    // FlatMap function which outputs a pair of users interacting, every time a pair is complete
    // State maintained in HashMap where still postId is key. Necessary to know the owner of a parent event
    //  so as to register interactions.
    public static final class ComputeInteractions
            extends RichFlatMapFunction<Tuple5<Long, Long, Integer, Character, Long>,
            Tuple3<Integer, Integer, Long>>
    {

        // key: eventId
        // value: ownerId, list(interactors on this post), list(interactionTimestamps)
        private HashMap<Long, Tuple3<Integer, List<Integer>, List<Long>>> interactions;

        @Override
        public void open(Configuration parameters) throws Exception{
            interactions = new HashMap<>();
        }

        @Override
        public void flatMap(Tuple5<Long, Long, Integer, Character, Long> in,
                            Collector<Tuple3<Integer, Integer, Long>> out) throws Exception{

            Long eventId = in.f0;
            Long responseToEventId = in.f1;
            Integer ownerId = in.f2;
            Character eventType = in.f3;
            Long eventTimestamp = in.f4;

            if(eventType == 'p')
            {
                if(interactions.containsKey(eventId)){
                    // Case: Child comment arrived before post
                    Tuple3<Integer, List<Integer>, List<Long>> oldValue = interactions.get(eventId);
                    int numberOfPendingValues = oldValue.f1.size();
                    for(int i=0; i< numberOfPendingValues; i++){
                        out.collect(new Tuple3<>(
                                ownerId,
                                oldValue.f1.get(i),
                                oldValue.f2.get(i)
                        ));
                    }

                    // Update ownerId in HashMap and clear both lists
                    interactions.put(eventId,
                            new Tuple3<>(ownerId, new ArrayList<>(), new ArrayList<>())
                    );
                }else{
                    // Case: Parent Post is first
                    interactions.put(eventId,
                            new Tuple3<>(ownerId, new ArrayList<>(), new ArrayList<>())
                    );
                }
            }

            if(eventType == 'c')
            {
                if(interactions.containsKey(eventId)){
                    // Case: Child reply arrived before comment
                    Tuple3<Integer, List<Integer>, List<Long>> oldValue = interactions.get(eventId);
                    int numberOfPendingValues = oldValue.f1.size();
                    for(int i=0; i< numberOfPendingValues; i++){
                        out.collect(new Tuple3<>(
                                ownerId,
                                oldValue.f1.get(i),
                                oldValue.f2.get(i)
                        ));
                    }

                    // Update ownerId in HashMap and clear both lists
                    interactions.put(eventId,
                            new Tuple3<>(ownerId, new ArrayList<>(), new ArrayList<>())
                    );
                }else{
                    // Case: Parent Comment is first
                    List<Integer> emptyList = new ArrayList<>();
                    interactions.put(eventId,
                            new Tuple3<>(ownerId, new ArrayList<>(), new ArrayList<>())
                    );
                }

                // Every comment/reply is an interaction between a post-comment, comment-reply
                // So update interactors list also

                if(interactions.containsKey(responseToEventId)){
                    // Case: Parent event already arrived
                    Tuple3<Integer, List<Integer>, List<Long>> oldValue = interactions.get(responseToEventId);
                    if(oldValue.f0!= -1){
                        // Valid pair
                        out.collect(new Tuple3<>(
                                oldValue.f0,
                                ownerId,
                                eventTimestamp
                        ));
                    }else {
                        // Add to wait buffer till ownerId is known
                        List<Integer> waitingInteractors = oldValue.f1;
                        waitingInteractors.add(ownerId);
                        List<Long> waitingTimestamps = oldValue.f2;
                        waitingTimestamps.add(eventTimestamp);
                        interactions.put(responseToEventId,
                                new Tuple3<>(-1, waitingInteractors, waitingTimestamps)
                        );
                    }
                }else{
                    // Add parent event with unknown ownerId -1 to be updated later when it comes
                    List<Integer> waitingInteractors = new ArrayList<>();
                    waitingInteractors.add(ownerId);
                    List<Long> waitingTimestamps = new ArrayList<>();
                    waitingTimestamps.add(eventTimestamp);
                    interactions.put(responseToEventId,
                            new Tuple3<>(-1, waitingInteractors, waitingTimestamps)
                    );
                }
            }

            if(eventType == 'l')
            {
                if(interactions.containsKey(responseToEventId)){
                    // Case: Parent post already arrived
                    Tuple3<Integer, List<Integer>, List<Long>> oldValue = interactions.get(responseToEventId);
                    if(oldValue.f0!= -1){
                        // Valid pair
                        // Valid pair
                        out.collect(new Tuple3<>(
                                oldValue.f0,
                                ownerId,
                                eventTimestamp
                        ));
                    }else {
                        // Add to wait buffer till ownerId is known
                        List<Integer> waitingInteractors = oldValue.f1;
                        waitingInteractors.add(ownerId);
                        List<Long> waitingTimestamps = oldValue.f2;
                        waitingTimestamps.add(eventTimestamp);
                        interactions.put(responseToEventId,
                                new Tuple3<>(-1, waitingInteractors, waitingTimestamps)
                        );
                    }
                }else{
                    // Add parent post with unknown ownerId -1 to be updated later when it comes
                    List<Integer> waitingInteractors = new ArrayList<>();
                    waitingInteractors.add(ownerId);
                    List<Long> waitingTimestamps = new ArrayList<>();
                    waitingTimestamps.add(eventTimestamp);
                    interactions.put(responseToEventId,
                            new Tuple3<>(-1, waitingInteractors, waitingTimestamps)
                    );
                }
            }
        }
    }

    // Uses dynamic and static data to provide recommendations
    // The commented code is for debugging purpose; also provides alternatives on how contenders can be chosen
    private static class GetRecommendationsProcessFunction extends
            ProcessAllWindowFunction<Tuple3<Integer, Integer, Long>,
                    Tuple3<String, String, HashMap<Integer, List<SimilarityScore>>>,
                    TimeWindow>
    {
        //Key: Pair of interactors (personId, personId)
        //Value: List of timestamps of interactions between the two
        private HashMap<Tuple2<Integer, Integer>, List<Long>> activeInteractions;

        //Fixed person.ids to recommend whom-to-follow
        //Randomly chosen based on ones that appear to show up initially
        final List<Integer> fixedIds = Arrays.asList( 546, 122, 735, 763, 30, 49, 602, 576, 21, 49);
        //List<Integer> fixedIds;

        //Static data for similarityMetric calculation
        //Enhancement: Ideally stored in a cache/in-memory DB
        private HashMap<Integer, List<Integer>> friendsMap;
        private HashMap<Integer, List<Integer>> interestsTagMap;
        private HashMap<Integer, Integer> organizationMap;
        private HashMap<Integer, Integer> locationMap;

        @Override
        public void open(Configuration parameters) throws Exception{
            activeInteractions = new HashMap<>();

            // Static data for similarityMetric calculation
            String[] staticFileNames = {
                    "./data/task_2/person_knows_person.csv", // to rule out those already friends
                    "./data/task_2/person_hasInterest_tag.csv", // similar tastes
                    "./data/task_2/person_studyAt_organisation.csv", // recommend alumnis
                    "./data/task_2/person_isLocatedIn_place.csv" // geographical factor
            };

            friendsMap = buildHashMap2(staticFileNames[0]);
            interestsTagMap = buildHashMap2(staticFileNames[1]);
            organizationMap = buildHashMap1(staticFileNames[2]);
            locationMap = buildHashMap1(staticFileNames[3]);
            /*
            System.out.println("SIZE:" + friendsMap.size() + " " + interestsTagMap.size() + " " +
            + organizationMap.size() + " " + locationMap.size());
            */

            // Choose 10 fixedIds randomly in the beginning
            /*
            fixedIds = new ArrayList<>();
            Random rand = new Random();
            for(int i = 0; i < 10; i++){
                fixedIds.add(rand.nextInt(1000));
            }*/
        }

        public HashMap<Integer, Integer> buildHashMap1(String filename) throws IOException{
            Path myPath = Paths.get(filename);
            HashMap<Integer, Integer> map = new HashMap<>();

            CSVParser parser = new CSVParserBuilder().withSeparator('|').build();

            try (BufferedReader br = Files.newBufferedReader(myPath,
                    StandardCharsets.UTF_8);
                 CSVReader reader = new CSVReaderBuilder(br).withCSVParser(parser)
                         .withSkipLines(1)
                         .build()) {
                List<String[]> rows = reader.readAll();
                for (String[] row : rows) {
                    map.put(Integer.parseInt(row[0]), Integer.parseInt(row[1]));
                }
            }
            return map;
        }

        public HashMap<Integer, List<Integer>> buildHashMap2(String filename) throws IOException{
            Path myPath = Paths.get(filename);
            HashMap<Integer, List<Integer>> map = new HashMap<>();

            CSVParser parser = new CSVParserBuilder().withSeparator('|').build();

            try (BufferedReader br = Files.newBufferedReader(myPath,
                    StandardCharsets.UTF_8);
                 CSVReader reader = new CSVReaderBuilder(br).withCSVParser(parser)
                         .withSkipLines(1)
                         .build()) {
                List<String[]> rows = reader.readAll();
                for (String[] row : rows) {
                    Integer key = Integer.parseInt(row[0]);
                    if(map.containsKey(key)){
                        List<Integer> newList = map.get(key);
                        newList.add(Integer.parseInt(row[1]));
                        map.put(Integer.parseInt(row[0]), newList);
                    }else{
                        List<Integer> newList = new ArrayList<>();
                        newList.add(Integer.parseInt(row[1]));
                        map.put(Integer.parseInt(row[0]), newList);
                    }
                }
            }
            return map;
        }

        @Override
        public void process(Context context,
                            Iterable<Tuple3<Integer, Integer, Long>> iterable,
                            Collector<Tuple3<String, String, HashMap<Integer, List<SimilarityScore>>>> collector) throws Exception {

            // Get window stats
            long windowStartTime = context.window().getStart();
            long windowEndTime = context.window().getEnd();
            // System.out.println(windowStartTime +" " + windowStartTime);

            //Update HashMap, remove outdated entries
            //Outdated here means those interactions that have happened before the past 4 hours from the window end time

            if(!activeInteractions.isEmpty()){
                ArrayList<Tuple2<Integer, Integer>> dropPairs = new ArrayList<>();
                ArrayList<Tuple3<Integer, Integer, List<Long>>> addPairs = new ArrayList<>();

                Iterator<Map.Entry<Tuple2<Integer, Integer>, List<Long>>> it =
                        activeInteractions.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry<Tuple2<Integer, Integer>, List<Long>> pair = it.next();
                    Tuple2<Integer, Integer> key= pair.getKey();
                    List<Long> timestamps = pair.getValue();
                    List<Long> updatedTimestamps = new ArrayList<>();
                    for (int i=0; i < timestamps.size(); i++){
                        if(timestamps.get(i) >= (windowEndTime - 4*60*60*1000)){
                            updatedTimestamps.add(timestamps.get(i));
                        }
                    }

                    if(updatedTimestamps.size() == 0){
                        dropPairs.add(key);
                    }else{
                        addPairs.add(new Tuple3<>(key.f0, key.f1, updatedTimestamps));
                    }
                }

                // Remove all outdated interactions
                activeInteractions.keySet().removeAll(dropPairs);
                // Add updated values
                for (Tuple3<Integer, Integer, List<Long>> t: addPairs){
                    activeInteractions.put(new Tuple2<>(t.f0, t.f1), t.f2);
                }
            }

            // Add new interactions to HashMap (Add both pairs - two-way interaction)
            for(Tuple3<Integer, Integer, Long> t: iterable){
                Tuple2<Integer, Integer> newKey1 = new Tuple2<>(t.f0, t.f1);
                Tuple2<Integer, Integer> newKey2 = new Tuple2<>(t.f1, t.f0); // for easy search
                Long newTimestamp = t.f2;

                if(activeInteractions.containsKey(newKey1)){
                    List<Long> newValue = activeInteractions.get(newKey1);
                    newValue.add(newTimestamp);
                    activeInteractions.put(newKey1, newValue);
                }else {
                    List<Long> newValue = new ArrayList<>();
                    newValue.add(newTimestamp);
                    activeInteractions.put(newKey1, newValue);
                }

                if(activeInteractions.containsKey(newKey2)){
                    List<Long> newValue = activeInteractions.get(newKey2);
                    newValue.add(newTimestamp);
                    activeInteractions.put(newKey2, newValue);
                }else {
                    List<Long> newValue = new ArrayList<>();
                    newValue.add(newTimestamp);
                    activeInteractions.put(newKey2, newValue);
                }
            }

            /*Check valid interaction pairs
            System.out.println(getDateString(context.window().getStart()) + " " +
                    getDateString(context.window().getEnd())+ " " +
                    activeInteractions.keySet());
            */

            // Prepare list of contenders who were recently associated with an activity online to compute score for
            /*
            List<Integer> onlineUsers = new ArrayList<>();

            for (Tuple2<Integer, Integer> key : activeInteractions.keySet()) {
                if(!fixedIds.contains(key.f1))
                    onlineUsers.add(key.f1);
            }

            // consider random contenders if not atleast 5 contenders available online
            Random rand = new Random();
            int numberOfUsers = onlineUsers.size();
            while((10 - numberOfUsers) > 0){
                int n = rand.nextInt(1000);
                if(!fixedIds.contains(n)){
                    onlineUsers.add(n);
                    numberOfUsers++;
                }
            }*/


            // Compute scores for all other users
            // This is an alternative approach taken to the above commented part since it was found that
            // not enough online activity was happening to see scores > 0
            // Maintain same variable name for ease of switching between the two approaches
            List<Integer> onlineUsers = new ArrayList<>();
            for(int i=1; i<=1000;i++)
            {
                if(!fixedIds.contains(i))
                    onlineUsers.add(i);
            }

            /*
            System.out.println(getDateString(context.window().getStart()) + " " +
                    getDateString(context.window().getEnd())+ "\n" +
                    "Fixed: " + fixedIds + "\n" + "Contenders: " + onlineUsers);
            */

            /*
            for(Integer person1: fixedIds){
                for(Integer person2: onlineUsers){
                    Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> score =
                            computeSimilarityScore(person1, person2);
                    System.out.println("P1 : " + person1 + " P2: " + person2 + " " + score);
                }
            }*/

            //Output results as a HashMap(personId, their top 5 recommendations as an arrayList)
            HashMap<Integer, List<SimilarityScore>> recommendationsMap = new HashMap<>();

            for(Integer person1: fixedIds){
                List<SimilarityScore> resultSet = new ArrayList<>();
                for(Integer person2: onlineUsers){
                    Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> score =
                            computeSimilarityScore(person1, person2);
                    SimilarityScore newScore = new SimilarityScore(person1,person2, score);
                    //System.out.println("P1 : " + person1 + " P2: " + person2 + " " + score);
                    resultSet.add(newScore);
                }
                Collections.sort(resultSet);

                List<SimilarityScore> output = new ArrayList<>();
                for(SimilarityScore s: resultSet.subList(0,5)){
                    output.add(s);
                }

                recommendationsMap.put(person1, output);
            }

            collector.collect(new Tuple3<>(getDateString(context.window().getStart()),
                    getDateString(context.window().getEnd()),
                    recommendationsMap));
        }

        public Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> computeSimilarityScore(int person1, int person2)
        {
            // Features considered

            int currentlyOnline = 0;
            int numOfOnlineInteractions = 0;
            int sameLocation = 0;
            int sameOrganisation = 0;
            int numOfCommonTags = 0;
            int alreadyAFriend = 0;

            int finalSimilarityScore = 0;

            Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> similarityTuple = new Tuple7<>();


            List<Integer> currentOnlineUsers = new ArrayList<>();

            for (Tuple2<Integer, Integer> key1 : activeInteractions.keySet()) {
                if(!fixedIds.contains(key1.f1))
                    currentOnlineUsers.add(key1.f1);
            }

            if(currentOnlineUsers.contains(person2)){
                currentlyOnline = 30;
            }

            Tuple2<Integer, Integer> key = new Tuple2<>();
            key.f0 = person1; key.f1 = person2;

            if(activeInteractions.containsKey(key))
            {
                numOfOnlineInteractions = activeInteractions.get(key).size();
            }

            if(locationMap.containsKey(person1) && locationMap.containsKey(person2)){
                sameLocation = (locationMap.get(person1).equals(locationMap.get(person2)))? 1 : 0;
            }

            if(organizationMap.containsKey(person1) && organizationMap.containsKey(person2)){
                sameOrganisation = (organizationMap.get(person1).equals(organizationMap.get(person2)))? 1 : 0;
            }

            if(interestsTagMap.containsKey(person1) && interestsTagMap.containsKey(person2)){
                List<Integer> person1Likes = interestsTagMap.get(person1);
                List<Integer> person2Likes = interestsTagMap.get(person2);

                person1Likes.retainAll(person2Likes);

                numOfCommonTags = person1Likes.size();
            }

            if(friendsMap.containsKey(person1)){
                List<Integer> person1Friends = friendsMap.get(person1);

                if(person1Friends.contains(person2)){
                    alreadyAFriend = -10000; // Want this to be a least priority recommendation
                }
            }

            finalSimilarityScore = currentlyOnline //one-time weightage : 30 points
                            + 30*(numOfOnlineInteractions) //weightage : 3
                            + 20 * (sameLocation) //weightage : 2
                            + 10* (sameOrganisation) //weightage : 1
                            + 20 * (numOfCommonTags) //weightage: 2 for each common tag
                            + alreadyAFriend;

            similarityTuple.f0 = currentlyOnline;
            similarityTuple.f1 = numOfOnlineInteractions;
            similarityTuple.f2 = sameLocation;
            similarityTuple.f3 = sameOrganisation;
            similarityTuple.f4 = numOfCommonTags;
            similarityTuple.f5 = alreadyAFriend;
            similarityTuple.f6 = finalSimilarityScore;

            return similarityTuple;
        }

        public static String getDateString(long windowTime){

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date dt = new Date(windowTime);
            return sdf.format(dt);
        }

    }
}

