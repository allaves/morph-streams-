package es.upm.fi.oeg.bolt;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Before;
import org.junit.Test;

public class TestLatencyObserverBolt {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		String m1 = "[2015-09-15T14:53:58+00:00, 2015-08-22T18:00:00.000, 502.0, water_management, Rabbit_Marsh, http://www.sense-t.csiro.au/sensorcloud/v1/network/water_management/platform/Rabbit_Marsh/sensor/AWS1-Campbell-CR800-logger, RECORD-Hourly, null, null, null]";
		String m2 = "[2015-09-15T14:53:58+00:00, 2015-08-22T18:00:00.000, 0.0, water_management, Rabbit_Marsh, http://www.sense-t.csiro.au/sensorcloud/v1/network/water_management/platform/Rabbit_Marsh/sensor/AWS1-Campbell-CS701-rain-gauge, Rainfall_Tot-Hourly, null, null, http://sweet.jpl.nasa.gov/2.3/sweetAll.owl#Rainfall]";
		// Convert system time to xsd:dateTime
		long arrivalTime = System.currentTimeMillis();
		String arrivalTimeStr = DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.format(arrivalTime);
		String messageId = "messageId";
		long processingTime = 0;
		long latency = 0;
		try {
			processingTime = (dateFormat.parse(m1.split(",")[0].substring(1))).getTime();
			// Latency in milliseconds
			latency = arrivalTime - processingTime;
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		System.out.println(latency + ", " + arrivalTimeStr + ", " + m1 + ", " + messageId);
	}

}
