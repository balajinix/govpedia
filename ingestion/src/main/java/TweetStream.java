import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by balaji on 15/11/15.
 * This is near exact copy of SampleStreamExample in hosebird example code.
 * Replicating this to understand what is going on, each line.
 */
public class TweetStream {

    public static void run(String consumerKey, String consumerSecret,
                           String accessToken, String accessTokenSecret) throws InterruptedException {

        // lets create a blocking queue - why blocking queue?
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // end point
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        // now ouath - the old basic auth is probably not supported anymore
        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        // basic client - it says by default gzip is enabled - gzip where?
        BasicClient twitterClient = new ClientBuilder()
                .name("govpediaClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        twitterClient.connect();

        /*
        // creating a client to write to elastic search
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient jestClient = jestClientFactory.getObject();

        // lets create an index in elastic search
        try {
            jestClient.execute(new CreateIndex.Builder("tweets").build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        */
        // read messages
        JSONParser parser = new JSONParser();
        for (int msgRead=0; msgRead < 100000; msgRead++) {
            if (twitterClient.isDone()) {
                System.out.println("Client connection closed unexpectedly" + twitterClient.getExitEvent().getMessage());
                break;
            }

            // now lets poll the queue
            String msg = queue.poll(1, TimeUnit.SECONDS);
            if (msg == null) {
                System.out.println("Did not receive message in 5 seconds");
            } else {
                Object obj = null;
                try {
                    obj = parser.parse(msg);
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }
                try {
                    JSONObject jsonObject = (JSONObject) obj;
                    String lang = (String) jsonObject.get("lang");
                    // we only want english tweets
                    if (lang != null && !lang.equals("en")) {
                        continue;
                    }
                    String text = (String) jsonObject.get("text");
                    if (text == null) continue;
                    if (!text.contains("CPBlr") &&
                            !text.contains("Government") &&
                            !text.contains("government") &&
                            !text.contains(".gov.") &&
                            !text.contains("PMOIndia")) {
                        continue;
                    }
                    String doc = text;
                    /*
                    String id = (String) jsonObject.get("id_str");
                    String userStr = (String) jsonObject.get("user");
                    if (userStr == null) {
                        continue;
                    }
                    JSONObject userObj = (JSONObject) parser.parse(userStr);

                    String userId = (String) userObj.get("id_str");
                    String screenName = (String) userObj.get("screen_name");

                    String doc = text + " - @" + screenName + " ref: " + "http://twitter.com/" + screenName + "/" + id;
                    */
                    System.out.println(doc);
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }

        twitterClient.stop();
        System.out.printf("Read %d messages\n", twitterClient.getStatsTracker().getNumMessages());
    }

    public static void main(String[] args) {
        Properties prop = new Properties();
        InputStream configFileInputStream = null;

        String consumerKey = "";
        String consumerSecret = "";
        String accessToken = "";
        String accessTokenSecret = "";

        try {
            String filename = "config.properties";
            configFileInputStream = TweetStream.class.getClassLoader().getResourceAsStream(filename);
            if (configFileInputStream == null) {
                System.out.println("File not found: " + filename);
            }
            //configFileInputStream = new FileInputStream("config.properties");
            prop.load(configFileInputStream);
            consumerKey = prop.getProperty("consumerKey");
            consumerSecret = prop.getProperty("consumerSecret");
            accessToken = prop.getProperty("accessToken");
            accessTokenSecret = prop.getProperty("accessTokenSecret");
        } catch (java.io.IOException e) {
            System.exit(0);
            e.printStackTrace();
        }

        try {
            TweetStream.run(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
