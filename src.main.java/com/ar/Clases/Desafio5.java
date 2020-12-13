package com.ar.Clases;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class Desafio5 {
	
	public static Dataset<Row> ejecutarDesafio5() {
		
		SparkConf sparkConf = new SparkConf().setAppName("DesafioNubimetrics");
		sparkConf.setMaster("local");
	    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

		 Dataset desa4 = Desafio4.ejecutarDesafio4();
		 Dataset csv =  sparkSession.read().csv("resources/visits.csv");
			 
		 Dataset join = desa4.join(csv,csv.col("_c0").equalTo(desa4.col("itemId")))
				 .select(desa4.col("itemId"),desa4.col("soldQuantity"),csv.col("_c1").alias("visits"));

		 return join.filter(join.col("soldQuantity").$greater(0));
		
	}
}
