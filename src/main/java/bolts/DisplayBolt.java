package bolts;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.lang.Long;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class DisplayBolt extends BaseRichBolt
{
  // Create a Redis Connection
  transient RedisConnection<String,String> redis;

  @Override
  public void prepare( Map conf,TopologyContext topologyContext,OutputCollector outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple)
  {
  
    String word = tuple.getStringByField("hashtag");

    Long count = tuple.getLongByField("count");
    // publish the hashtag to HashtagTopology
    redis.publish("HashtagTopology", word + "|" + Long.toString(count));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {}
}
