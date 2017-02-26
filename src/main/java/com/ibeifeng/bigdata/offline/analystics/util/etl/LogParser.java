package com.ibeifeng.bigdata.offline.analystics.util.etl;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants;
import com.ibeifeng.bigdata.offline.analystics.util.IPSeekerExt;
import com.ibeifeng.bigdata.offline.analystics.util.IPSeekerExt.RegionInfo;
import com.ibeifeng.bigdata.offline.analystics.util.UserAgentUtil;
import com.ibeifeng.bigdata.offline.analystics.util.UserAgentUtil.UserAgentInfo;

/**
 * -》按照^A进行分割
	-》ip
	-》s_time
	-》hostname
	-》URI
-》解析URI，按照？进行分割
	-》请求的资源
	-》每个事件收集的字段
-》解析事件，按照&进行分割
	-》key=value
-》key=value进行解析，按照=进行分割
	-》key：字段的名称
	-》value：字段名称对应的值
	-》对value进行解码
-》字段的补全
	-》浏览器和操作系统信息
		-》b_iev
		-》浏览器名称、浏览器版本、操作系统名称、操作系统的版本
	-》ip解析
		-》国家、省份、城市
		-》纯真ip数据库，淘宝ip库
		-》企业中：基于纯真开发自己的ip库，如果纯真无法满足，再调用淘宝
-》字段存储:Map
	put 'tbname','rowkey','cf:col','value'
 * @author beifeng
 *
 */

public class LogParser {
	
	private Logger logger = LoggerFactory.getLogger(LogParser.class);
	
	public Map<String, String> handleLogParser(String logText) throws UnsupportedEncodingException{
		
		//new map instance
		Map<String,String> logInfo = new HashMap<String,String>();
		//analysis the logText
		if(StringUtils.isNotBlank(logText)){
			String[] splits = logText.split("\\^A");
			if(splits.length == 4){
				//define the ip, time, hostname ,uri
				String ip = splits[0];
				String s_time = splits[1];
				String hostname = splits[2];
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_IP, ip);
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,s_time);
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_HTTP_HOST, hostname);
				String uri = splits[3];
				//解析URI
				this.AnalysisURI(uri,logInfo);
				//解析客户端
				this.AnalysisUserAgent(logInfo);
				//解析ip
				this.AnalysisIP(ip,logInfo);

			}
			
			
		}else{
			logger.error("记录格式不正确："+logText);
//			logger.info(msg);
//			logger.warn(msg);
		}

		return logInfo;
		
	}

	
	/**
	 * 用于解析ip地址
	 * 	-》国家
	 * 	-》省份
	 * 	-》城市
	 * @param ip
	 * @param logInfo
	 */
	public void AnalysisIP(String ip, Map<String, String> logInfo) {
		// TODO Auto-generated method stub
		if(StringUtils.isNotBlank(ip)){
			IPSeekerExt ipseek = IPSeekerExt.getInstance();
			RegionInfo regionInfo = ipseek.analyseIp(ip);
			String country = regionInfo.getCountry();
			String province = regionInfo.getProvince();
			String city = regionInfo.getCity();
			if(StringUtils.isNotBlank(country)){
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, country);
			}
			if(StringUtils.isNotBlank(province)){
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, province);
			}
			if(StringUtils.isNotBlank(city)){
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, city);
			}
		}
	}

	/**
	 * 用于处理分析用户客户端
	 * 	-》浏览器：名称，版本
	 * 	-》操作系统：名称，版本
	 * @param logInfo
	 */
	public void AnalysisUserAgent(Map<String, String> logInfo) {
		// TODO Auto-generated method stub
		//获取用户客户端信息
		String agent = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
		UserAgentInfo userAgent = UserAgentUtil.analyticUserAgent(agent);
		String browsername = userAgent.getBrowserName();
		String browserversion = userAgent.getBrowserVersion();
		String osname = userAgent.getOsName();
		String osversion = userAgent.getOsVersion();
		if(StringUtils.isNotBlank(browsername)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, browsername);
		}
		if(StringUtils.isNotBlank(browserversion)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, browserversion);
		}
		if(StringUtils.isNotBlank(osname)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, osname);
		}
		if(StringUtils.isNotBlank(osversion)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, osversion);
		}
	}


	/**
	 * 用于解析URI
	 * @param uri
	 * @param logInfo
	 * @throws UnsupportedEncodingException 
	 */
	public void AnalysisURI(String uri, Map<String, String> logInfo) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		if(StringUtils.isNotBlank(uri)){
			String[] splits = uri.split("\\?");
			if(splits.length == 2){
				String e_source = splits[1];
				String[] keyvalues = e_source.split("&");
				for(String keyvalue : keyvalues){
					String key = keyvalue.split("=")[0];
					String value = keyvalue.split("=")[1];
					String realvalue = URLDecoder.decode(value, "utf-8");
					logInfo.put(key, realvalue);
				}
				
			}
		}
	}

}









