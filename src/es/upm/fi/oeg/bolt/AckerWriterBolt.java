package es.upm.fi.oeg.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.time.DateFormatUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class AckerWriterBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2389491699560673415L;
	private OutputCollector collector;
	private File log;
	private BufferedWriter writer;
	
	public AckerWriterBolt(String filePath) {
		log = new File(filePath);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
		try {
			writer = new BufferedWriter(new FileWriter(log, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void execute(Tuple tuple) {	
		try {
			writer.newLine();
			writer.append(tuple.getStringByField("sensorCloudObs"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
	
	@Override
	public void cleanup() {
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
