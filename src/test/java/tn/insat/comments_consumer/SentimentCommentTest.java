package tn.insat.comments_consumer;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
class SentimentCommentTest {

        @Test
        void testSentimentComment() {
            String comment = "comment_id,parent_id,'comment , body',subreddit,timestamp,sentiment";
            SentimentComment sentimentComment = new SentimentComment(comment);
            assertEquals("comment_id", sentimentComment.comment_id);
            assertEquals("parent_id", sentimentComment.parent_id);
            assertEquals("'comment , body'", sentimentComment.comment_body);
            assertEquals("subreddit", sentimentComment.subreddit);
            assertEquals("timestamp", sentimentComment.timestamp);
            assertEquals("sentiment", sentimentComment.sentiment);
        }

}