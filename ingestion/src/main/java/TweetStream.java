import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by balaji on 15/11/15.
 */
public class TweetStream {
    public static void run(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        // lets create a blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // end point
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
    }
}
