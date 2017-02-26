package com.ibeifeng.bigdata.offline.analystics.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.ibeifeng.bigdata.offline.analystics.converter.IDimensionConverter;
import com.ibeifeng.bigdata.offline.analystics.converter.impl.DimensionConverterImpl;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.base.KpiDimension;
/**
 * 根据KPI信息获取kpi的维度id
 * @author ad
 *
 */
public class KpiDimensionConverterUDF extends UDF {
	
	private IDimensionConverter converter;
	
	public KpiDimensionConverterUDF(){
		converter = new DimensionConverterImpl();
	}
	
	public IntWritable evaluate(Text kpiName) throws IOException{
		KpiDimension dimension = new KpiDimension(kpiName.toString());
		return new IntWritable(converter.getDimensionIdByValue(dimension));
	}

}
