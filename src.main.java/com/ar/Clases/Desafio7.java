package com.ar.Clases;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Desafio7 {
	
	public static Dataset ejecutarDesafio7() {
		
		SparkConf sparkConf = new SparkConf().setAppName("Test");
		sparkConf.setMaster("local");
	    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
	  	    
	    Dataset<Row> resp4 = Desafio4.ejecutarDesafio4();
     	Dataset<?> filtroColumnas = resp4.select(resp4.col("itemId"),resp4.col("availableQuantity"));
		 
		 filtroColumnas.createOrReplaceTempView("datos");
		 String query = "SELECT "
		 		+ "itemId,"
		 		+ "availableQuantity, "
		 		+ "ROUND(100.0 * availableQuantity / (SELECT SUM(availableQuantity) FROM datos), 2) as stockPercentage "
		 		+ "from datos order by availableQuantity desc ";
		 return sparkSession.sql(query);

		
	}
}
