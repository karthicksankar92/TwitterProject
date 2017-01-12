package bolts;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.Arrays;
import java.lang.String;
import java.util.List;




public class TwitterNormalizer extends BaseRichBolt 
{
  OutputCollector collector;
  // Remove words that do not have much importance
  String[] stopWords={"a","am","the","this","is","are","not","have","had","you","your","this","that","with",
      "I'm","and","de","http","http:","https","https:","http://","https://","https://t","they","their","you're","don't",
      "bien","take","pero","Pero","esta","follows","followed","like","video","retweet","RETWEET","FOLLOW",
      "Like","todo","them","want","it's","can't","como","make","really","would","could","should","when",
      "were","en","una","in","on","go","all","up","la","watch","followers","me","que","more","do","new","de",
      "just","much","what","which","how","las","te","es","is","my","from","about"};
  
 	
  @Override
	public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) 
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }
  
  // This method gets the tweets, parses them into words, removes stopwords and emits words that begin with Hashtag
  @Override
	public void execute(Tuple input) 
  {
    String tweets =input.getString(0);
    String[] hashwords=tweets.split("[ .,?!]+");
    for(String hashword: hashwords)
      {
        if(hashword.length()>3 && !Arrays.asList(stopWords).contains(hashword))
         {
           if(hashword.startsWith("#"))
           	 {
                collector.emit(new Values(hashword));
              }
          }
      }
  }
	

	/**
	 * The bolt will only emit the field "hash" 
	 */
  @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
  {
		declarer.declare(new Fields("hash"));
	}
}
