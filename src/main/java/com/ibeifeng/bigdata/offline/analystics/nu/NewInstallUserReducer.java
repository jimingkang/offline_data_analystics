package com.ibeifeng.bigdata.offline.analystics.nu;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.ibeifeng.bigdata.offline.analystics.common.KpiType;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.stats.StatsUserDimension;
import com.ibeifeng.bigdata.offline.analystics.dimension.value.MapWritableValue;

/**
 * 新增用户统计Reducer
 * @author ibeifeng
 *
 */
public class NewInstallUserReducer extends Reducer<StatsUserDimension, Text,
			StatsDimension,MapWritableValue> {
	
	private Set<String> uuids = new HashSet<String>();
	
	private  MapWritableValue mapWritableValue = new MapWritableValue();
	@Override
	protected void reduce(StatsUserDimension key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		for(Text v : values){
			// 去重
			uuids.add(v.toString());
		}
		MapWritable mapWritable = new MapWritable();
		// 计数
		mapWritable.put(new IntWritable(-1), new IntWritable(uuids.size()));
		
		mapWritableValue.setValue(mapWritable);
		String kpiName = key.getStatsCommon().getKpi().getKpiName();
		if(KpiType.NEW_INSTALL_USER.name.equals(kpiName)){
			mapWritableValue.setKpi(KpiType.NEW_INSTALL_USER);
		}else if(KpiType.BROWSER_NEW_INSTALL_USER.name.equals(kpiName)){
			mapWritableValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
		}
		
		context.write(key, mapWritableValue);
	}

}
