package es.upm.fi.oeg.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.esotericsoftware.minlog.Log;
import com.esotericsoftware.minlog.Log.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Converts Sensor Cloud messages to field-named tuples
 */
public class SensorCloudParserBolt extends BaseRichBolt {

	private OutputCollector collector;
	private HashMap<String, String[]> platformLocationCache;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.platformLocationCache = new HashMap<String, String[]>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// Regular message: "<sample time="2015-04-13T11:11:59.000" value="100.0" sensor="libelium.356893904.356893904-9247P-Sensiron-SHT75-air.relative-humidity"/>";
		// Tag message: "<sample time="2015-05-20T23:00:00.000" value="0.6" sensor="bom_gov_au.94963.rain.rain_trace" tags="au.csiro.ict.tasman.processor.TopicRewrite: bom_gov_au.94963.rain.rain_trace[80]"/>"
		// Gets the string body message
		String message = tuple.getString(0);
		String[] messageArray = message.split(" ");
		// Tag messages ignored
		if (messageArray.length <= 4) {
			String observationSamplingTime = messageArray[1].split("\"")[1];
			String value = messageArray[2].split("\"")[1];
			String[] path = messageArray[3].split("\"")[1].split("\\.");
			String network = path[0];
			String platform = path[1];
			String platformUrl = "http://www.sense-t.csiro.au/sensorcloud/v1/network/" + network + "/platform/" + platform;
			String sensor = path[2];
			String sensorUrl = platformUrl + "/sensor/" + sensor;
			String phenomenon = path[3];		// Observed property
			
			String lat = null;
			String lon = null;
			if (!(platformLocationCache.containsKey(platformUrl))) {
				// Get platform location from API
				String line = "";
				String objString = "";
				JSONObject jsonObj = null;
				try {
					URLConnection connection = new URL(platformUrl + "/deployment").openConnection();
					BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
					while ((line = br.readLine()) != null) {
						objString += line;
					}
					JSONParser jsonParser = new JSONParser();
					jsonObj = (JSONObject) jsonParser.parse(objString);
					JSONArray deploymentArray = (JSONArray) jsonObj.get("deployment");
					// Some platforms do not have a deployment attached
					if (!deploymentArray.isEmpty()) {
						JSONObject hrefObj = (JSONObject) deploymentArray.get(0);
						String deploymentUrl = (String) hrefObj.get("href");
						br.close();
						// New connection
						connection = new URL(deploymentUrl).openConnection();
						System.out.println("### TEST ###: " + deploymentUrl);
						br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
						objString = "";
						while ((line = br.readLine()) != null) {
							objString += line;
						}
						jsonObj = (JSONObject) jsonParser.parse(objString);
						JSONObject platformObj = (JSONObject) jsonObj.get("sfl:PlatformDeployment");
						JSONObject locationObj = (JSONObject) platformObj.get("sfl:deploymentLocation");
						JSONObject pointObj = (JSONObject) locationObj.get("gml:Point");
						// TODO: get srsName and include CRS in the tuples
						//JSONObject attributeObj = (JSONObject) pointObj.get("attribute");
						//String srs = ...
						
						// e.g. gml:pos: "147.0075 -43.3167"
						String location = (String) pointObj.get("gml:pos");
						String[] latLon = location.split(" "); 
						lat = latLon[0];
						lon = latLon[1];
						platformLocationCache.put(platformUrl, new String[]{lat, lon});
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else {
				lat = platformLocationCache.get(platformUrl)[0];
				lon = platformLocationCache.get(platformUrl)[1];
			}
			
			// Convert system time to xsd:dateTime
			String observationResultTime = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(System.currentTimeMillis());
			// Emit values
			collector.emit(new Values(observationResultTime, observationSamplingTime, value, network, platform, sensorUrl, phenomenon, lat, lon));
		}
		//else {
			// TODO: Handle tag messages
			// String tag = messageArray[4].split("\"")[1];
		//}
		// Ack tuple
		collector.ack(tuple);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO: include CRS in the tuple
		declarer.declare(new Fields("observationResultTime", "observationSamplingTime", "value", "network", "platform", "sensor", "phenomenon", "lat", "lon"));
	}

}
