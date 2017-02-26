package com.ibeifeng.bigdata.offline.analystics.nu;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants;
import com.ibeifeng.bigdata.offline.analystics.common.EventLogConstants.EventEnum;
import com.ibeifeng.bigdata.offline.analystics.common.GlobalConstants;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsUserDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.value.MapWritableValue;
import com.ibeifeng.bigdata.offline.analystics.transformer.MySQLOutputFormat;
import com.ibeifeng.bigdata.offline.analystics.util.TimeUtil;


/**
 * 新增用户统计驱动类
 * 
 * @author ibeifeng
 *
 */
public class NewInstallUserRunner implements Tool {
	
	public static void main(final String[] args) {
		
		// MapReduce 读写HDFS文件 涉及权限问题
		
		//System.setProperty("HADOOP_USER_NAME", "beifeng");
		
		UserGroupInformation.createRemoteUser("beifeng").doAs(new PrivilegedAction<Object>() {

			@Override
			public Object run() {
				try {
					int exitCode = ToolRunner.run(new NewInstallUserRunner(), args);
					if(exitCode == 0){
						System.out.print("任务执行成功。。。。");
					}
					
					return exitCode;
				} catch (Exception e) {
					e.printStackTrace();
				}
				return -1;
			}
		});
		
	}

	private static final Logger logger = LoggerFactory.getLogger(NewInstallUserRunner.class);

	private Configuration conf = new Configuration();
	
	private static final byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;

	@Override
	public void setConf(Configuration conf) {
		// 本地运行
		conf.set("fs.defaultFS", "hdfs://hadoop-senior01.ibeifeng.com:8020");
		conf.set("yarn.resourcemanager.hostname", "hadoop-senior01.ibeifeng.com");
		conf.set("hbase.zookeeper.quorum", "hadoop-senior01.ibeifeng.com:2181");
		//////

		// 加载资源文件
		// 输出器  给sql中？赋值的collector类名称
		conf.addResource("output-collector.xml");
		// 输出结果到mysql的执行sql更新语句
		conf.addResource("query-mapping.xml");
		// 数据库连接参数
		conf.addResource("transformer-env.xml");
		this.conf = HBaseConfiguration.create(conf);

	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// 解析args，从中获取出处理日期
		this.processArgs(args, conf);

		Job job = Job.getInstance(conf, "new_install_user");

		job.setJarByClass(NewInstallUserRunner.class);
		List<Scan> scans = new ArrayList<Scan>();
		Scan scan = new Scan();
		
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES); // yyyy-MM-dd
		// event_logsyyyyMMdd
		
		String hbaseTableName = 
				EventLogConstants.HBASE_NAME_EVENT_LOGS + 
			TimeUtil.parseLong2String(
					TimeUtil.parseString2Long(date, "yyyy-MM-dd"),"yyyyMMdd");
		// 指定获取数据的HBase表
		scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(hbaseTableName));
		
		// 按天分表，无需添加起始rowkey和终止rowkey
		
		// 如果不是按天分表，需要设置起始rowkey和终止rowkey
		
		//scan.setStartRow(startRow);
		//scan.setStopRow(stopRow);
		
		// 给scan添加过滤器
		// 列标签
		FilterList filterList = new FilterList();
		
		// 添加过滤事件类型为LAUNCH事件的记录过滤器
		filterList.addFilter(new SingleColumnValueFilter(family, 
				Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), 
				CompareOp.EQUAL, Bytes.toBytes(EventEnum.LAUNCH.name)));
		
		// 从刚才过滤出的行中过滤出所关心的列标签
		byte[][] prefixes = {
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME),
			Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION),
		};
		filterList.addFilter(new  MultipleColumnPrefixFilter(prefixes));
		
		scan.setFilter(filterList);
		
		scans.add(scan);
		
		// 集群运行    addDependencyJars 默认值true
		/*TableMapReduceUtil.initTableMapperJob(scans, NewInstallUserMapper.class, 
				StatsUserDimension.class, Text.class, job);*/
		// 本地运行 addDependencyJars 设为false
		TableMapReduceUtil.initTableMapperJob(scans, NewInstallUserMapper.class, 
				StatsUserDimension.class, Text.class, job,false);
		
		job.setReducerClass(NewInstallUserReducer.class);
		job.setOutputKeyClass(StatsDimension.class);
		job.setOutputValueClass(MapWritableValue.class);
		
		
		job.setNumReduceTasks(4);  // 采样，确认维度种类 ，根据维度种类设置reducer个数
		
		job.setOutputFormatClass(MySQLOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * 
	 * @param args
	 * @param conf2
	 */
	private void processArgs(String[] args, Configuration conf) {
		String date = null;
		// -d yyyy-MM-dd
		if (args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if ("-d".equals(args[i])) {
					if (i + 1 < args.length) {
						date = args[i + 1];
						break;
					}
				}
			}
		}

		if (StringUtils.isBlank(date) && !TimeUtil.isValidateRunningDate(date)) {
			// 日期不对或者为空，默认昨天
			date = TimeUtil.getYesterday();
		}

		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}

}
