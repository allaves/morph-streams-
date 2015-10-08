package es.upm.fi.oeg.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import es.upm.fi.oeg.topology.SensorCloudParsingLatencyTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private BufferedReader reader;
	private String filePath;
	private String observationResultTime;
	
	public FileSpout(String filePath) {
		this.filePath = filePath;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		InputStream is = SensorCloudParsingLatencyTopology.class.getResourceAsStream(filePath);
		reader = new BufferedReader(new InputStreamReader(is));
	}

	@Override
	public void nextTuple() {
		try {
			String line = reader.readLine();
			if (line != null) {
				if (!line.isEmpty()) {
				observationResultTime = String.valueOf(System.currentTimeMillis());
				collector.emit(new Values(observationResultTime, line));
				Thread.sleep(1);
				}
			}
			else {
				reader.close();
				System.out.println("***THE END!***");
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("observationResultTime", "line"));
	}

}
