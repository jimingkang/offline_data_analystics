package com.ibeifeng.bigdata.offline.analystics.util.etl;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants;
import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants.EventEnum;
import com.ibeifeng.bigdata.offline.analystics.util.TimeUtil;

/**
 * 功能：
 * 		-》解析每一行数据
 * 		-》将所有的字段全部封装成put，用于写入hbase
 * @author beifeng
 *
 */
public class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
	
	private LogParser logParser = new LogParser();
	private CRC32 crc32 = new CRC32();
	private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//get the line
		String logText = value.toString();
		//analysis the line
		Map<String, String> logInfo = logParser.handleLogParser(logText);
		//get the rowkey
		String s_time = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
		long time = TimeUtil.parseNginxServerTime2Long(s_time);
		String u_ud = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
		String u_md = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
		String event_alias = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
		EventEnum ee = EventEnum.valueOfAlias(event_alias);
		switch (ee) {
		case LAUNCH:
		case PAGEVIEW:
		case CHARGEREQUEST:
		case CHARGESUCCESS:
		case CHARGEREFUND:
		case EVENT:
			String rowkey = this.CreateRowkey(time,u_ud,u_md,event_alias);
			Put put = new Put(Bytes.toBytes(rowkey));
			for(Map.Entry<String, String> maps: logInfo.entrySet()){
				put.add(
						family, 
						Bytes.toBytes(maps.getKey()), 
						Bytes.toBytes(maps.getValue())
						);
			}
			context.write(NullWritable.get(), put);
			break;

		default:
			return;
		}
		
		//create the instance of put
		
	}

	/**
	 * 用于生成rowkey
	 * @param time
	 * @param u_ud
	 * @param u_md
	 * @param event_alias
	 * @return
	 */
	public String CreateRowkey(long time, String u_ud, String u_md,
			String event_alias) {
		// TODO Auto-generated method stub
		StringBuilder sbuilder = new StringBuilder();
		sbuilder.append(time+"_");
		crc32.reset();
		if(StringUtils.isNotBlank(u_ud)){
			crc32.update(Bytes.toBytes(u_ud));
		}
		if(StringUtils.isNotBlank(u_md)){
			crc32.update(Bytes.toBytes(u_md));
		}
		if(StringUtils.isNotBlank(event_alias)){
			crc32.update(Bytes.toBytes(event_alias));
		}
		
		sbuilder.append(crc32.getValue() % 100000000L);
		return sbuilder.toString();
	}
	
	public static void main(String[] args) {
		System.out.println(
				new ETLMapper().CreateRowkey(1450569601351L, "4B16B8BB-D6AA-4118-87F8-C58680D22657", 
						"4B16B8BB", "e_l")
				
				);
	}
}
















