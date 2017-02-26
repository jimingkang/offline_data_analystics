package com.ibeifeng.bigdata.offline.analystics.transformer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibeifeng.bigdata.offline.analystics.common.GlobalConstants;
import com.ibeifeng.bigdata.offline.analystics.common.KpiType;
import com.ibeifeng.bigdata.offline.analystics.converter.IDimensionConverter;
import com.ibeifeng.bigdata.offline.analystics.converter.impl.DimensionConverterImpl;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.value.BaseStatsValueWritable;
import com.ibeifeng.bigdata.offline.analystics.util.JdbcManager;

/**
 * 自定义将MR任务的输出数据写到MySQL表中
 * @author ibeifeng
 *
 * key     
 * value
 * 是不是应该与 Reducer的输出类型一致
 */
public class MySQLOutputFormat extends 
	OutputFormat<StatsDimension,BaseStatsValueWritable>{
	
	private static final Logger logger = LoggerFactory.getLogger(MySQLOutputFormat.class);

	@Override
	public RecordWriter<StatsDimension,BaseStatsValueWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		
		// 需要有MySQL的连接
		// sql 
		// 将分析结果存入mysql时，统计结果表需要有维度ID，所以在插入数据时，需要先从维度表中将维度ID取到
		// IDimensionConverter 获取维度ID
		// 从MapReduce的应用上下文 context获取得到configuration对象，从对象中获取得到sql语句
		Configuration conf = context.getConfiguration();
		Connection conn = null;
		IDimensionConverter converter = new DimensionConverterImpl();
		try {
			conn = JdbcManager.getConnection(conf, "report");
			
			// 手动提交事务，所以需要关闭自动提交功能
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("获取数据库连接出现异常!!!! " , e );
			// 释放掉 converter 里面打开的资源
			converter.close();
			throw  new RuntimeException("获取数据库连接出现异常!!!!");
		}
		
		return new MySQLRecordWriter(conf,conn,converter);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// nothings
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
	
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
	}
	
	/**
	 * 输入key value的类型与外部类 一致
	 * @author ad
	 *
	 */
	protected static class MySQLRecordWriter extends 
		RecordWriter<StatsDimension,BaseStatsValueWritable>{
		
		private Connection conn = null;
		private Configuration conf ;
		private IDimensionConverter converter ;
		
		// 缓存需要执行的sql次数
		private Map<KpiType,Integer> batch = new HashMap<KpiType,Integer>();
		
		// 将构造出来的preparedStatement对象缓存下来
		private  Map<KpiType,PreparedStatement> pstms = new HashMap<KpiType,PreparedStatement>();

		public MySQLRecordWriter(Configuration conf, Connection conn, 
				IDimensionConverter converter) {
			this.conn = conn;
			this.conf = conf;
			this.converter = converter;
		}

		@Override
		public void write(StatsDimension key, BaseStatsValueWritable value) throws IOException, InterruptedException {
			// 将记录插入到数据库里面
			// sql
			KpiType kpiType = value.getKpi();
			String sql = conf.get(kpiType.name);
			
			// 批量执行   对PreparedStatement对象进行共用
			PreparedStatement pstm ;
			
			try {
				
				if(pstms.containsKey(kpiType)){
					pstm = pstms.get(kpiType);
				}else{
					pstm = conn.prepareStatement(sql);
					pstms.put(kpiType, pstm);
				}
				// 判断sql的次数是否已经达到可以批量执行的次数 （自己指定） 500
				int count = 1;
				if(batch.containsKey(kpiType)){
					count = batch.get(kpiType);
					count += 1;
				}
				
				batch.put(kpiType, count);
				
				//pstm = ?  需要给sql语句中的问号赋值
				//获取collector对象，并且通过collector对象的collect来进行给sql语句中的问号赋值
				String collectorClazzName = 
						conf.get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpiType.name);
				Class<?> collectorclazz = Class.forName(collectorClazzName);
				// collector的实现类一定要有的无参构造器
				IOutputCollector collector = 
						(IOutputCollector) collectorclazz.newInstance();
				collector.collect(conf, key, value, pstm, converter);
				
				if(count % 
						conf.getInt(GlobalConstants.JDBC_BATCH_NUMBER,
								GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER) == 0){
					// 批量执行
					pstm.executeBatch();
					// 清掉batch缓存的次数
					batch.remove(kpiType);
					// 手动提交事务
					conn.commit();
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("");
				
				// 要不要自动回滚   出现异常，明显说明代码有问题，分析重新做
				try {
					// 恢复自动提交
					conn.setAutoCommit(false);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				throw new RuntimeException("");
			} 
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// 收尾 释放资源
			// 对最后一次如果没有达到批量提交数量的sql，补充执行一次
			try {
				for(Map.Entry<KpiType, PreparedStatement> entry : pstms.entrySet()){
					// 对最后一次如果没有达到批量提交数量的sql，补充执行一次
					entry.getValue().executeBatch();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("");
				
				// 要不要自动回滚   出现异常，明显说明代码有问题，分析重新做
				try {
					// 恢复自动提交
					conn.setAutoCommit(false);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				throw new RuntimeException("");
			}finally{
				if(conn != null){
					// 事务提交
					try {
						conn.commit();
					} catch (SQLException e) {
					}
				}
				
				// 释放资源
				for(Map.Entry<KpiType, PreparedStatement> entry : pstms.entrySet()){
					try {
						entry.getValue().close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				
				JdbcManager.closeConnection(conn, null, null);
			}
		}
	}
}
