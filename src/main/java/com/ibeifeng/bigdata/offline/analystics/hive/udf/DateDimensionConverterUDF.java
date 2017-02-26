package com.ibeifeng.bigdata.offline.analystics.hive.udf;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.ibeifeng.bigdata.offline.analystics.common.DateEnum;
import com.ibeifeng.bigdata.offline.analystics.converter.IDimensionConverter;
import com.ibeifeng.bigdata.offline.analystics.converter.impl.DimensionConverterImpl;
import com.ibeifeng.bigdata.offline.analystics.dimension.key.base.DateDimension;
/**
 * 日期维度ID获取
 * @author ad
 *
 */
public class DateDimensionConverterUDF extends UDF{
	
	private IDimensionConverter converter;
	
	public DateDimensionConverterUDF(){
		converter = new DimensionConverterImpl();
	}
	
	public IntWritable evaluate(LongWritable serverTime , Text dateType) throws IOException{
		DateDimension dimension = DateDimension.buildDate(serverTime.get(), 
				DateEnum.valueOfName(dateType.toString()));
		return new IntWritable(converter.getDimensionIdByValue(dimension));
	}
	
	public IntWritable evaluate(Text dateStr , Text dateType) throws IOException, ParseException{
		//2015-12-20
		Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr.toString());
		DateDimension dimension = DateDimension.buildDate(date.getTime(), 
				DateEnum.valueOfName(dateType.toString()));
		return new IntWritable(converter.getDimensionIdByValue(dimension));
	}

} 
