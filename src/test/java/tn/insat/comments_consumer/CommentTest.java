package tn.insat.comments_consumer;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
class CommentTest {

        @Test
        void testComment() {
            String comment = "comment_id,parent_id,'comment , body',subreddit,timestamp";
            Comment comment1 = new Comment(comment);
            assertEquals("comment_id", comment1.comment_id);
            assertEquals("parent_id", comment1.parent_id);
            assertEquals("'comment , body'", comment1.comment_body);
            assertEquals("subreddit", comment1.subreddit);
            assertEquals("timestamp", comment1.timestamp);
        }

}