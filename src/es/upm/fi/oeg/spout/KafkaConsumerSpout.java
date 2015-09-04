package es.upm.fi.oeg.spout;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import es.upm.fi.oeg.stream.StreamHandler;
import kafka.javaapi.consumer.ConsumerConnector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class KafkaConsumerSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private String topic;
	
	public KafkaConsumerSpout(String topic) {
		this.topic = topic;  
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		StreamHandler.getInstance().getKafkaConsumer().subscribe(topic);
		
	}

	@Override
	public void nextTuple() {
		Map<String, ConsumerRecords<String, String>> records = StreamHandler.getInstance().getKafkaConsumer().poll(100);
		for (String topic : records.keySet()) {
			
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
	


}
