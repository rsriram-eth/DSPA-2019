/*
Class to add posts from csv file into Kafka
 */

package poststats.kafkaProducer;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import poststats.datatypes.PostEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PostAdder extends RichSourceFunction<PostEvent> {

    static private long count = 0;

    @Override
    public void run(SourceContext<PostEvent> ctx) throws Exception {
        try {
            List<PostEvent> posts = readPostsFromCSV("./data/task_1/post_stream.csv");
            for (PostEvent p : posts) {
                ctx.collect(p);
            }
        } finally {
            System.out.println("Post count = " + PostAdder.count);
        }
    }

    @Override
    public void cancel() {}

    private static List<PostEvent> readPostsFromCSV(String fileName) {
        List<PostEvent> posts = new ArrayList<>();
        Path pathToFile = Paths.get(fileName);

        try (BufferedReader br = Files.newBufferedReader(pathToFile,
                StandardCharsets.US_ASCII)) {

            String line = br.readLine(); //header
            line = br.readLine();

            while (line != null) {
                String[] attributes = line.split("\\|");
                PostEvent post = createPostEvent(attributes);
                posts.add(post);
                PostAdder.count += 1;
                line = br.readLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return posts;
    }

    private static PostEvent createPostEvent(String[] metadata) {
        // System.out.println(metadata[0]+" "+metadata[1]+" "+metadata[2]);
        long postId = Long.parseLong(metadata[0]);
        int personId = Integer.parseInt(metadata[1]);
        String creationDate = metadata[2];

        // create and return post of this metadata
        return new PostEvent(creationDate, personId, 'p', postId);
    }

}
