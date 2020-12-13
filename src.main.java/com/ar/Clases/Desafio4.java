package com.ar.Clases;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;



public class Desafio4 {
	
	public static Dataset<Row> ejecutarDesafio4() {
		SparkConf sparkConf = new SparkConf().setAppName("DesafioNubimetrics");
		sparkConf.setMaster("local");
	    SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
	  	    
	    Dataset ds =  sparkSession.read().json("resources/MPE1004.json");
	    
		 Dataset<?> resp = ds.select(ds.col("site_id"),  functions.explode(ds.col("results")));
		 
		 //TODO Buscar funcion no deprecada para asignar id incremental a las	 
		 return resp.select(functions.monotonicallyIncreasingId().alias("rowId"),
				 resp.col("col").getField("id").alias("itemId"),
				 resp.col("col").getField("sold_quantity").alias("soldQuantity"),
				 resp.col("col").getField("available_quantity").alias("availableQuantity"));
		 		 
	    
	    
		
	}
	


	
}
