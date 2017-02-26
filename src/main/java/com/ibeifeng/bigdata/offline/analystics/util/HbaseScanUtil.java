package com.ibeifeng.bigdata.offline.analystics.util;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants;
import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants.EventEnum;

/**
 * -》hbase与MapReduce集成，从hbase中读取数据
-》通过hbase实现过滤：u_ud,s_time,pl,browsername,browserversion,en（e_l）
	-》hbase的过滤器
		-》列标签的过滤
		-》对value的过滤

 * @author beifeng
 *
 */
public class HbaseScanUtil {

	private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
	
	public Scan scanFilterUtil(String date){
		
		//get a scan instance
		Scan scan = new Scan();
		String tablename = EventLogConstants.HBASE_NAME_EVENT_LOGS+date;
		scan.setAttribute(scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tablename));
		//值的过滤
		FilterList filterlist = new FilterList();
		String qualifier = EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME;
		String launch = EventEnum.LAUNCH.alias;
		Filter valuefilter = new SingleColumnValueFilter(
				family, 
				Bytes.toBytes(qualifier), 
				CompareOp.EQUAL, 
				Bytes.toBytes(launch)
				);
		filterlist.addFilter(valuefilter);
		//列标签的过滤：u_ud,s_time,pl,browsername,browserversion,en
		byte[][] prefixs= {
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
		};
		Filter columnFilter = new MultipleColumnPrefixFilter(prefixs);
		filterlist.addFilter(columnFilter);
		scan.setFilter(filterlist);

		return scan;
		
	}
}
