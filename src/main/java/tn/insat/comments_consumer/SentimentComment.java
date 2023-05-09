package tn.insat.comments_consumer;

import java.util.Arrays;
public class SentimentComment {
    public String sentiment;
    public String comment_id;
    public String parent_id;
    public String comment_body;
    public String subreddit;
    public String timestamp;

    public SentimentComment(String comment) {
        String[] comment_parts = comment.split(",");
        this.comment_id = comment_parts[0];
        this.parent_id = comment_parts[1];
        // body may contain commas
        this.comment_body = String.join(",", Arrays.copyOfRange(comment_parts, 2, comment_parts.length-3));
        this.subreddit = comment_parts[comment_parts.length-3];
        this.timestamp = comment_parts[comment_parts.length-2];
        this.sentiment = comment_parts[comment_parts.length-1];
    }
}
