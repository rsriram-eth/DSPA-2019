/*
Class to add comments from csv file into Kafka
 */

package poststats.kafkaProducer;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import poststats.datatypes.CommentEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CommentAdder extends RichSourceFunction<CommentEvent> {

    static private long count = 0;

    @Override
    public void run(SourceContext<CommentEvent> ctx) throws Exception {
        try {
            List<CommentEvent> comments = readCommentsFromCSV("./data/task_1/comment_stream.csv");
            for (CommentEvent c : comments) {
                ctx.collect(c);
            }
        } finally {
            System.out.println("Comment count = " + CommentAdder.count);
        }
    }

    @Override
    public void cancel() {}

    private static List<CommentEvent> readCommentsFromCSV(String fileName) {
        List<CommentEvent> comments = new ArrayList<>();
        Path pathToFile = Paths.get(fileName);

        try (BufferedReader br = Files.newBufferedReader(pathToFile,
                StandardCharsets.US_ASCII)) {

            String line = br.readLine(); //header
            line = br.readLine();

            while (line != null) {
                String[] attributes = line.split("\\|");
                CommentEvent comment = createCommentEvent(attributes);
                comments.add(comment);
                CommentAdder.count += 1;
                line = br.readLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return comments;
    }

    private static CommentEvent createCommentEvent(String[] metadata) {
        // System.out.println(metadata[0]+" "+metadata[1]+" "+metadata[2]+" "+metadata[3]+" "+metadata[4]);
        long commentId = Long.parseLong(metadata[0]);
        int personId = Integer.parseInt(metadata[1]);
        String creationDate = metadata[2];
        long postId = Long.parseLong(metadata[3]);
        long replyId = Long.parseLong(metadata[4]);

        // create and return comment of this metadata
        return new CommentEvent(creationDate, personId, 'c', postId, commentId, replyId);
    }

}
