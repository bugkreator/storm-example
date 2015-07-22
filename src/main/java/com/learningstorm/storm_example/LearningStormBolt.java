package com.learningstorm.storm_example;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

public class LearningStormBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;

	private String fileName = "";
	private SimpleDateFormat df = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

	@Override
	public void prepare(Map stormConf, TopologyContext context)
	{
		super.prepare(stormConf, context);

		fileName = "/tmp/bolt_output_" + (new SimpleDateFormat("YYYYMMdd_HHmmss")).format(Calendar.getInstance().getTimeInMillis()) +
				"_" + context.getThisComponentId() + "_" + context.getThisTaskId() + "_" + java.util.UUID.randomUUID().toString().replace("-","");
	}


	private void writeToLog(String data)
	{
		String line = df.format(Calendar.getInstance().getTimeInMillis()) + " : " + data;
		FileWriter fw = null;
		try
		{
			fw = new FileWriter(fileName, true);
			fw.write(line + "\r\n");
		}
		catch (Exception e) {
			//swallow
		}
		finally {
			try {
				fw.close();
			}
			catch (Exception e)
			{
				//
			}
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// Get the field "site" from input tuple.
		String test = input.getStringByField("site");
		// print the value of field "site" on console.
		writeToLog("Name of input site is : " + test);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}
