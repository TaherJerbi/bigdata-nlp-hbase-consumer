package tn.insat.comments_consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
public class HbaseRepository {
    // singleton instance
    private static HbaseRepository instance = null;
    private static Configuration config = null;
    private static Connection connection = null;
    private static Admin admin = null;
    private static Table raw_comments_table = null;
    private static Table comments_sentiments_table = null;
    private static final String RAW_COMMENTS_TABLE_NAME = "comments_raw";
    private static final String COMMENTS_SENTIMENTS_TABLE_NAME = "comments_sentiments";

    // private constructor
    private HbaseRepository() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            raw_comments_table = connection.getTable(TableName.valueOf(RAW_COMMENTS_TABLE_NAME));
            comments_sentiments_table = connection.getTable(TableName.valueOf(COMMENTS_SENTIMENTS_TABLE_NAME));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // public static method to get the singleton instance
    public static HbaseRepository getInstance() {
        if (instance == null) {
            instance = new HbaseRepository();
        }
        return instance;
    }

    // insert a raw comment into HBase
    // arg: comment (comment_id, parent_id, comment_body, subreddit, timestamp)
    public void insertRawComment(String comment) {
        try {
            // create the row key
            String rowKey = String.valueOf(System.currentTimeMillis());
            Put put = new Put(Bytes.toBytes(rowKey));

            Comment commentObj = new Comment(comment);
            // Add the columns and values for the new record
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("comment_id"), Bytes.toBytes(commentObj.comment_id));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("parent_id"), Bytes.toBytes(commentObj.parent_id));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("comment_body"), Bytes.toBytes(commentObj.comment_body));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("subreddit"), Bytes.toBytes(commentObj.subreddit));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timestamp"), Bytes.toBytes(commentObj.timestamp));

            raw_comments_table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // insert a comment sentiment into HBase
    // arg: comment (comment_id, parent_id, comment_body, subreddit, timestamp, sentiment)
    public void insertCommentSentiment(String comment) {
        try {
            // create the row key
            String rowKey = String.valueOf(System.currentTimeMillis());
            Put put = new Put(Bytes.toBytes(rowKey));

            SentimentComment commentObj = new SentimentComment(comment);

            // Add the columns and values for the new record
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("comment_id"), Bytes.toBytes(commentObj.comment_id));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("parent_id"), Bytes.toBytes(commentObj.parent_id));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("comment_body"), Bytes.toBytes(commentObj.comment_body));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("subreddit"), Bytes.toBytes(commentObj.subreddit));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("timestamp"), Bytes.toBytes(commentObj.timestamp));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sentiment"), Bytes.toBytes(commentObj.sentiment));

            comments_sentiments_table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
