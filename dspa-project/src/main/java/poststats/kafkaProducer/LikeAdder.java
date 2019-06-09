/*
Class to add likes from csv file into Kafka
 */

package poststats.kafkaProducer;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import poststats.datatypes.LikeEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class LikeAdder extends RichSourceFunction<LikeEvent> {

    static private long count = 0;

    @Override
    public void run(SourceContext<LikeEvent> ctx) throws Exception {
        try {
            List<LikeEvent> likes = readLikesFromCSV("./data/task_1/like_stream.csv");
            for (LikeEvent l : likes) {
                ctx.collect(l);
            }
        } finally {
            System.out.println("Like count = " + LikeAdder.count);
        }
    }

    @Override
    public void cancel() {}

    private static List<LikeEvent> readLikesFromCSV(String fileName) {
        List<LikeEvent> likes = new ArrayList<>();
        Path pathToFile = Paths.get(fileName);

        try (BufferedReader br = Files.newBufferedReader(pathToFile,
                StandardCharsets.US_ASCII)) {

            String line = br.readLine(); //header
            line = br.readLine();

            while (line != null) {
                String[] attributes = line.split("\\|");
                LikeEvent like = createLikeEvent(attributes);
                likes.add(like);
                LikeAdder.count += 1;
                line = br.readLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return likes;
    }

    private static LikeEvent createLikeEvent(String[] metadata) {
        // System.out.println(metadata[0]+" "+metadata[1]+" "+metadata[2]);
        int personId = Integer.parseInt(metadata[0]);
        long postId = Long.parseLong(metadata[1]);
        String creationDate = metadata[2];

        // create and return like of this metadata
        return new LikeEvent(creationDate, personId, 'l', postId);
    }

}
