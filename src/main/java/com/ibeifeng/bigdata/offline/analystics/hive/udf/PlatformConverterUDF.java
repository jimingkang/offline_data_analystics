package com.ibeifeng.bigdata.offline.analystics.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.ibeifeng.bigdata.offline.analystics.converter.IDimensionConverter;
import com.ibeifeng.bigdata.offline.analystics.converter.impl.DimensionConverterImpl;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.base.PlatformDimension;
/**
 * 根据平台维度信息获取平台维度ID  UDF
 * @author ad
 *
 */
public class PlatformConverterUDF extends UDF{
	
	private IDimensionConverter converter;
	
	public PlatformConverterUDF(){
		
		converter = new DimensionConverterImpl();
	}
	
	public IntWritable evaluate(Text plName, Text version) throws IOException{
		PlatformDimension dimension = 
				new PlatformDimension(plName.toString(), version.toString());
		return new IntWritable(converter.getDimensionIdByValue(dimension));
	}

}
