package com.ibeifeng.bigdata.offline.analystics.nu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.ibeifeng.bigdata.offline.analystics.common.GlobalConstants;
import com.ibeifeng.bigdata.offline.analystics.converter.IDimensionConverter;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.BaseDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsUserDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.value.BaseStatsValueWritable;
import com.ibeifeng.bigdata.offline.analystics.dimension.value.MapWritableValue;
import com.ibeifeng.bigdata.offline.analystics.transformer.IOutputCollector;
/**
 * 
 * @author ad
 *
 */
public class StatsDeviceBrowserNewInstallUserCollector implements IOutputCollector {

	@Override
	public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
			PreparedStatement pstmt,
			IDimensionConverter converter) throws IOException, SQLException {
		/* 
		INSERT INTO `stats_device_browser`(
			`platform_dimension_id`,
			`date_dimension_id`,
			`browser_dimension_id`,
			`new_install_users`,
			`created`)
			VALUES(?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `new_install_users` = ?
		*/
		StatsUserDimension statsUserDimension = (StatsUserDimension) key;
		MapWritableValue mapWritableValue = (MapWritableValue) value;
		
		IntWritable newInstallUsers = 
				(IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));
		int i = 0;
		pstmt.setInt(++ i,
				converter.getDimensionIdByValue(
						statsUserDimension.getStatsCommon().getPlatform()));
		pstmt.setInt(++ i,
				converter.getDimensionIdByValue(
						statsUserDimension.getStatsCommon().getDate()));
		
		pstmt.setInt(++ i,
				converter.getDimensionIdByValue(
						statsUserDimension.getBrowser()));
		
		pstmt.setInt(++ i,newInstallUsers.get());
		
		pstmt.setString( ++ i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
		
		pstmt.setInt(++i,newInstallUsers.get());
		
		// 添加到批量执行中
		pstmt.addBatch();
		
	}

}
