package es.upm.fi.oeg.spout;

import java.io.IOException;
import java.util.Map;

import net.sf.ehcache.constructs.nonstop.NonstopThreadPool;

import org.apache.commons.lang3.time.DateFormatUtils;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import es.upm.fi.oeg.stream.Stream;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SensorCloudSpout extends BaseRichSpout {
	private String host;
	
	private SpoutOutputCollector collector;
	private Map conf;
	private QueueingConsumer consumer;

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(conf.get("host").toString());
		factory.setPort(60195);
		factory.setUsername(conf.get("user").toString());
		factory.setPassword(conf.get("password").toString());
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(conf.get("queue").toString(), true, consumer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		try {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			// Convert system time to xsd:dateTime - No need for this
			//String observationResultTime = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(System.currentTimeMillis());
			
			String observationResultTime = String.valueOf(System.currentTimeMillis());
			String data = new String(delivery.getBody());
			collector.emit(new Values(observationResultTime, data));
		} catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
			e.printStackTrace();
		}	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("observationResultTime", "sensorCloudObs"));
	}

}
