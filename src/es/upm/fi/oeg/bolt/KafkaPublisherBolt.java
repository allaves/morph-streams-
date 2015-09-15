package es.upm.fi.oeg.bolt;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;

import es.upm.fi.oeg.stream.StreamHandler;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class KafkaPublisherBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private ProducerRecord<String, Object> record;
	

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// If the tuple has the observedProperty annotated, e.g. http://sweet.jpl.nasa.gov/2.3/sweetAll.owl#Temperature
		if (tuple.getStringByField("observedProperty") != null) {
			// We get the name of the SWEET to assign it to the channel (Kafka does not allow to create topic names with strange characters)
			String[] splittedObservedProperty = tuple.getStringByField("observedProperty").split("#");
			int lastIndex = splittedObservedProperty.length;
			String topic = splittedObservedProperty[lastIndex-1];
			// Creates a new kafka record with the annotated observed property as a topic (channel)
			record = new ProducerRecord<String, Object>(topic, tuple.getValues().toString());
			// Sends message to Kafka
			StreamHandler.getInstance().getKafkaProducer().send(record);
		}
		// Creates a new kafka generic record
		record = new ProducerRecord<String, Object>("all", tuple.getValues().toString());
		// Sends message to Kafka
		StreamHandler.getInstance().getKafkaProducer().send(record);
		
		//collector.emit(tuple.getValues());
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("observationResultTime", "observationSamplingTime", "value", "network", "platform", "sensor", "phenomenon", "lat", "lon", "observedProperty"));
	}

}
