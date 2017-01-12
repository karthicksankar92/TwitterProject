package bolts;
import java.util.HashMap;
import java.util.Map;
import java.lang.Long;
import java.lang.String;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import backtype.storm.topology.base.BaseRichBolt;

public class TwitterCounter extends BaseRichBolt
{
 	OutputCollector collector;
	
	// Create a hash map of type <String,Long> for counters which keeps track of the hash tag count
	Map<String, Long> counters;

	@Override
	public void prepare(Map map, TopologyContext context,OutputCollector  outputCollector) 
	{
		collector = outputCollector;
		counters = new HashMap<String, Long>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
		declarer.declare(new Fields("hashtag","count"));
	}

	// If the hashtags are already present in the hashmap it increments the hashtag by 1, else it creates a new field in the hash map and emits them as <hashtag,count> pair
	@Override
	public void execute(Tuple input) 
	{
	    String str = input.getString(0);
		if(!counters.containsKey(str))
		{
			counters.put(str, 1L);
		}
		else
		{
			Long c = counters.get(str) + 1;
			counters.put(str, c);
		}
		collector.emit(new Values(str,counters.get(str)));
	}
}
