import spouts.TwitterReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.TwitterCounter;
import bolts.TwitterNormalizer;
import bolts.DisplayBolt;
import backtype.storm.utils.Utils;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();

		//Twitter credentials
		TwitterReader twitterreader=new TwitterReader(
			"xxxxxxxxxxxxxxxxx", //ConsumerKey
			"xxxxxxxxxxxxxxxxxx",//Consumer Secret
			"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",//Access Token
			"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");//Access Token Secret

		//attach Twitter Reader spout to Topology 
		builder.setSpout("twitter-reader",twitterreader,1);
		//attach Twitter Normalizer bolt to Topology with parallelism 15 using Shuffle Grouping
		builder.setBolt("twitter-normalizer", new TwitterNormalizer(),15).shuffleGrouping("twitter-reader");
		//attach Twitter Counter bolt to Topology with parallelism 10 using Fields Grouping
		builder.setBolt("twitter-counter", new TwitterCounter(),10).fieldsGrouping("twitter-normalizer", new Fields("hash"));
		//attach display bolt to Topology with parallelism 1 using global Grouping
		builder.setBolt("display-bolt", new DisplayBolt(),1).globalGrouping("twitter-counter");
		
        //Configuration
		Config conf = new Config();
		
		conf.setDebug(true);
        //Topology run
		
		conf.setMaxTaskParallelism(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Most-Trending-hashtags-Topology", conf, builder.createTopology());
		Utils.sleep(300000);
		cluster.shutdown();
	}
}
