package com.ar.Clases;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Desafio6 {
	
	public static Dataset<Row> ejecutarDesafio6() {
			 
		 Dataset<Row> desa5 = Desafio5.ejecutarDesafio5();
		 Dataset<?> resp2 = desa5.selectExpr("*",
				 desa5.col("soldQuantity").$div(desa5.col("visits")).alias("conversionRate").toString()
				 ).sort(functions.desc("conversionRate"));
		 
         WindowSpec conversionRate = Window.orderBy(functions.desc("conversionRate"));
         return resp2.withColumn("conversionRanking", functions.rank().over(conversionRate));
	}
}
