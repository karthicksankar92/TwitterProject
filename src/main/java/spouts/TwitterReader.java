package spouts;
import backtype.storm.utils.Utils;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
//--------------------------------------------------------------
//Import Twitter4j java library to integrate with Twitter service
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;
//--------------------------------------------------------------
import java.util.concurrent.LinkedBlockingQueue;
import java.lang.String;
//--------------------------------------------------------------

public class TwitterReader extends BaseRichSpout 
{
	//Initialize Twitter credentials
	String cons_key;		
	String cons_secret;
	String acc_key;
	String acc_tok;
	SpoutOutputCollector collector;
	
	// Twitter stream to get tweets
  	TwitterStream tweetStream;

	// Initialize the queue to get the tweets
	LinkedBlockingQueue<String> tweet_queue = null;
  
  	// TwitterListener- To get the tweets from Streaming API
  	private class TwitterListener implements StatusListener
  	{
	    @Override
	    public void onStatus(Status status) 
	    {
	      // add the tweet into the queue buffer
	      tweet_queue.offer(status.getText());
	    }

	    @Override
	    public void onDeletionNotice(StatusDeletionNotice sdn) 
	    {
	    }

	    @Override
	    public void onTrackLimitationNotice(int i) 
	    {
	    }

	    @Override
	    public void onScrubGeo(long l, long l1) 
	    {
	    }

	    @Override
	    public void onStallWarning(StallWarning warning) 
	    {
	    }

	    @Override
	    public void onException(Exception e) 
	    {
	      e.printStackTrace();
	    }
    };

    // constructor to get the credential details from TopologyMain
	public  TwitterReader(String consumer_key, String consumer_secret, String access_key, String access_token)
	{
		cons_key=consumer_key; 			// Consumer Key
		cons_secret=consumer_secret;    // Consumer Secret Key
		acc_key=access_key;				// Access Token Key
		acc_tok=access_token;			// Access Token secret Key

	}
	

	// NextTuple is used to emit a tuple when it gets a stream, else it sleeps for 50 ms to allow other methods to work
	@Override
	public void nextTuple() 
	{
		// The poll() method is used to retrieve and remove the head of this queue, or returns null if this queue is empty.
		String retur_tweets=tweet_queue.poll();
		if(retur_tweets==null)
		{
			Utils.sleep(50);
			return;
		} 
		collector.emit(new Values(retur_tweets));
	}

	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector spoutcollector)
	{
		tweet_queue = new LinkedBlockingQueue<String>(1000);

    	// save the output collector for emitting tuples
    	collector = spoutcollector;

    	// Using twitter crendential build configuration
    	ConfigurationBuilder cb = new ConfigurationBuilder()
               .setOAuthConsumerKey(cons_key)
               .setOAuthConsumerSecret(cons_secret)
               .setOAuthAccessToken(acc_key)
               .setOAuthAccessTokenSecret(acc_tok);

    
         // Initialize TwitterStreamFactory using configuration object
    	TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());

    	// get an instance of twitterstream
    	tweetStream = tf.getInstance();

    	// get the handler for twitter stream
    	tweetStream.addListener(new TwitterListener());

    	// start the sampling of tweets
    	tweetStream.sample();	
	}

	/**
	 * Declare the output field "tweets"
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
		declarer.declare(new Fields("tweets"));
	}
}
