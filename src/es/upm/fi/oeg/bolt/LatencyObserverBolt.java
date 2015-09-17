package es.upm.fi.oeg.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import javax.swing.text.DateFormatter;

import org.apache.commons.lang3.time.DateFormatUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LatencyObserverBolt extends BaseRichBolt {
	private OutputCollector collector;
	private static String FILE_PATH = "/tmp/latencyResults.txt";
	private SimpleDateFormat dateFormat;

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
		dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	}

	@Override
	public void execute(Tuple input) {
		// Convert system time to xsd:dateTime
		long arrivalTime = System.currentTimeMillis();
		String arrivalTimeStr = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(arrivalTime);
		String messageId = input.getMessageId().toString();
		String message = input.getValue(0).toString();
		long processingTime = 0;
		long latency = 0;
		try {
			processingTime = dateFormat.parse(message.split(",")[0].substring(1)).getTime();
			// Latency in milliseconds
			latency = arrivalTime - processingTime;
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH, true));
			writer.newLine();
			writer.append(latency + ", " + arrivalTimeStr + ", " + message + ", " + messageId);
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Actually, there is no message emitted
		declarer.declare(new Fields("arrivalTimestamp", "message"));
	}
	
//	@Override
//	public void cleanup() {
//		
//	}
	
	

}
