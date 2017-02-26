package com.ibeifeng.bigdata.offline.analystics.nu;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibeifeng.bigdata.offline.analystics.common.DateEnum;
import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants;
import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants.EventEnum;
import com.ibeifeng.bigdata.offline.analystics.common.KpiType;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.base.BrowserDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.base.DateDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.base.KpiDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.base.PlatformDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsCommonDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsUserDimension;
import com.ibeifeng.bigdata.offline.analystics.util.TimeUtil;

/**
 *新增用户统计Mapper
 * @author ibeifeng
 *
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, Text>{
	
	private static final Logger logger = LoggerFactory.getLogger(NewInstallUserMapper.class);
	
	private StatsUserDimension statsUserDimension = new StatsUserDimension();
	

	
	
	private static final byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
	private Text uuid = new Text();
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {
		
		String uid =  Bytes.toString(
				value.getValue(family, 
						Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
		
		// 获取日期信息
		String serverTime = Bytes.toString(
				value.getValue(family, 
						Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
		long time = TimeUtil.parseNginxServerTime2Long(serverTime);
		System.out.println(time);
		DateDimension dateDimension = DateDimension
				.buildDate(time, DateEnum.DAY);
		
		if(uid == null || serverTime == null){
			logger.error("");
			return;
		}

		
		uuid.set(uid);
		// 获取平台信息
		String  plName = Bytes.toString(
				value.getValue(family, 
						Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
		String plVer = Bytes.toString(
				value.getValue(family, 
						Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)));
		
		List<PlatformDimension> pfs = PlatformDimension.buildList(plName, plVer);
		
		String browserName =  Bytes.toString(
				value.getValue(family, 
						Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
		
		String browserVersion =   Bytes.toString(
				value.getValue(family, 
						Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
		
		List<BrowserDimension> brds = BrowserDimension.buildList(browserName, browserVersion);
		// 定义一个默认的
		BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
 		// 构造组合维度
		StatsCommonDimension statsCommonDimension = statsUserDimension.getStatsCommon();
		statsCommonDimension.setDate(dateDimension);
		statsUserDimension.setBrowser(defaultBrowserDimension);
		for(PlatformDimension pf : pfs){
			statsCommonDimension.setPlatform(pf);
			KpiDimension  kpiDimension = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
			statsCommonDimension.setKpi(kpiDimension);
			
			context.write(statsUserDimension, uuid);
			for(BrowserDimension brd : brds){
				statsUserDimension.setBrowser(brd);
				kpiDimension = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);
				statsCommonDimension.setKpi(kpiDimension);
				
				context.write(statsUserDimension, uuid);
			}
		}
	}
}
