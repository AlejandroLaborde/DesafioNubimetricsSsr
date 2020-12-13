package com.ar.Clases;


import java.time.LocalDate;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Desafio3 {

	public static Dataset<Row> ejecutarDesafio3( ) {

		LocalDate date = LocalDate.now();
    	SparkConf sparkConf = new SparkConf().setAppName("DesafioNubimetrics");
        sparkConf.setMaster("local");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<?> dataset =  sparkSession.read().json("resources/Sellers.json");
        dataset.createOrReplaceTempView("datos");
        String query = "SELECT body.site_id as siteId, body.id as sellerId , body.nickname as sellerNickname, body.points as sellerPoints from datos ";
        Dataset<Row> result = sparkSession.sql(query);
        result.show();
        
        String site = "";

        List<Row> lista = result.select("siteId").collectAsList();
        site = lista.get(0).getString(0);
	    
        String path = "Archivos/"+site+"/"+date.getMonth().getValue()+"/"+date.getYear()+"/"+date.getDayOfMonth()+"/";
        
        result.filter("sellerPoints > 0").write().mode("overwrite").option("header", true).csv(path+"Positivo");
        result.filter("sellerPoints = 0").write().mode("overwrite").option("header", true).csv(path+"Cero");
        result.filter("sellerPoints < 0").write().mode("overwrite").option("header", true).csv(path+"Negativo");
        
        return result;
	}     
        
	

}
