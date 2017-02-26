package com.ibeifeng.bigdata.offline.analystics.util.etl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants;
import com.ibeifeng.bigdata.offline.analystics.common.GlobalConstants;
import com.ibeifeng.bigdata.offline.analystics.util.TimeUtil;


/**
 * ETL的驱动类
 * 	-》程序的初始化
 * 	-》定义输入和输出
 * @author beifeng
 *
 */
public class ETLDriver implements Tool {

	private Configuration conf = new Configuration();
	private Logger logger = LoggerFactory.getLogger(ETLDriver.class);
	
	@Override
	public void setConf(Configuration that) {
		// TODO Auto-generated method stub
		//本地运行，集群运行时注释掉
		that.set("fs.defaultFS", "hdfs://192.168.199.198:9000");
		that.set("hbase.zookeeper.quorum", "192.168.199.198:2181");
		this.conf = HBaseConfiguration.create(that);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//job
		Job job = Job.getInstance(conf, "ETL-job");
		job.setJarByClass(ETLDriver.class);
		//input
		//parserDate = 2015-12-20
		String parserDate = this.getDate(args,conf);
		//输入文件路径：/eventLog/20151220/20151220.log
		long s_time = TimeUtil.parseString2Long(parserDate);
		String date = TimeUtil.parseLong2String(s_time, "yyyyMMdd");
		Path path = new Path(GlobalConstants.HDFS_LOGS_PATH_PREFIX+"/"+date);
		FileInputFormat.addInputPath(job, path);
		//map
		job.setMapperClass(ETLMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		//shuffle
//		job.setPartitionerClass(cls);
//		job.setCombinerClass(cls);
//		job.setSortComparatorClass(cls);
//		job.setCombinerKeyGroupingComparatorClass(cls);
//		job.setGroupingComparatorClass(cls);
		//reduce&output
		/**
		 * 与hbase集成，初始化mapper和reduce
		 * //TableMapReduceUtil.initTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, job);
		 */
		String tablename = this.getTableName(date,conf);
		//集群运行方式
		//TableMapReduceUtil.initTableReducerJob(tablename, null, job);
		//本地运行调用：addDependencyJars-是否添加依赖的jar包
		TableMapReduceUtil.initTableReducerJob(tablename, null, job, null, null, null, null, false);
		//submit
		boolean isSuccessed = job.waitForCompletion(true);
		
		
		return isSuccessed ? 0 : 1;
	}
	
	/**
	 * 表名：prefix+date
	 * 		-》判断表是否存在
	 * 		-》创建表
	 * @param date
	 * @param conf2
	 * @return
	 */
	public String getTableName(String date, Configuration conf) {
		// TODO Auto-generated method stub
		
		//hbase table name : event_logs20151220
		String tablename = EventLogConstants.HBASE_NAME_EVENT_LOGS+date;
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			if(admin.tableExists(tablename)){
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
			}
			//create 'tbname','info'
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tablename));
			HColumnDescriptor family = new HColumnDescriptor(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME);
			desc.addFamily(family);
			admin.createTable(desc);
			//admin.createTable(desc, splitKeys);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("hbase访问异常！");
			throw new RuntimeException(e);
		}finally {
			if(admin == null){
				try {
					admin.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		return tablename;
	}

	/**
	 * 获取日期方法：
	 * 		-》如果有参数：使用参数中的日期
	 * 		-》如果没有参数或者参数不合法，使用昨天的日期
	 * 		-》参数格式：-d 2015-12-20
	 * @param args
	 * @param conf2
	 * @return
	 */
	public String getDate(String[] args, Configuration conf) {
		// TODO Auto-generated method stub
		String date = null;
		for(int i =0 ;i<args.length;i++){
			if(args[i].equals("-d")){
				date = args[i+1];
			}
		}
		
		if(date == null || !TimeUtil.isValidateRunningDate(date)){
			date = TimeUtil.getYesterday(TimeUtil.DATE_FORMAT);
		}
		
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
		System.out.println("daya="+date);
		return date;
	}

	public static void main(String[] args) {
		try {
			int status = ToolRunner.run(new ETLDriver(), args);
			System.exit(status);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

}
