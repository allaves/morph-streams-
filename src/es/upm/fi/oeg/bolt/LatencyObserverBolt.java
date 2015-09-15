package es.upm.fi.oeg.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;

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
	private OutputStream os;
	private PrintStream printStream;

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
//		try {
//			os = new FileOutputStream(file);
//			printStream = new PrintStream(os);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public void execute(Tuple input) {
		// Convert system time to xsd:dateTime
		String arrivalTimestamp = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(System.currentTimeMillis());
		String message = input.getValue(0).toString();
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH, true));
			writer.newLine();
			writer.append(arrivalTimestamp + " - " + message);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//printStream.println(arrivalTimestamp + " - " + message);
		//collector.emit(new Values(arrivalTimestamp, message));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("arrivalTimestamp", "message"));
	}
	
//	@Override
//	public void cleanup() {
//		printStream.close();
//		try {
//			os.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	

}
