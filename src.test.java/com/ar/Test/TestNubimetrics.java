package com.ar.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;

import org.apache.parquet.format.StringType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.ar.Clases.Desafio2;
import com.ar.Clases.Desafio3;
import com.ar.Clases.Desafio4;
import com.ar.Clases.Desafio5;
import com.ar.Clases.Desafio6;
import com.ar.Clases.Desafio7;
import com.ar.Excepciones.ErrorConexionException;
import com.ar.Utilidades.ManejoArchivos;


class TestNubimetrics {


	@Test
	void test_1_desafio2_guardadoExitosoInformacion() {
		try {
			Desafio2.ejecutarDesafio2("Archivos","MLA","MLA1743");
			LocalDate date = LocalDate.now();
			String nombreCarpeta = "search" + "json" + date.getYear() + date.getMonth().getValue();
			assertTrue(ManejoArchivos.existeArchivo("Archivos/"+ nombreCarpeta+ "/response.json"));
		}catch (Exception e) {
			assertTrue(false);
		}
	}
	
	@Test
	void test_2_desafio2_excepcionPorFalleEnConexion() {
		try {
			Desafio2.ejecutarDesafio2("Archivos","ML","MLA1743");
			LocalDate date = LocalDate.now();
			String nombreCarpeta = "search" + "json" + date.getYear() + date.getMonth().getValue();
			assertTrue(ManejoArchivos.existeArchivo("Archivos/"+ nombreCarpeta+ "/response.json"));
		}catch (Exception e) {
			assertTrue(e.getClass().equals(ErrorConexionException.class));
			
		}
	}
	
	
	@Test
	void test_3_desafio3_deserealizacionCorrecta() {
		Dataset<Row> response = Desafio3.ejecutarDesafio3();
        StructType schema = new StructType()
        		.add("siteId","string")
        		.add("sellerId","long")
        		.add("sellerNickname","string")
        		.add("sellerPoints","long");
        response.printSchema();
		response.show();
        assertEquals(schema,response.schema());
	}
	
	@Test
	void test_4_desafio3_guardadoArchivosPositivoCorrecto() {
	    LocalDate date= LocalDate.now();
		Dataset<Row> response = Desafio3.ejecutarDesafio3();
		List<Row> lista = response.select("siteId").collectAsList();
	    String site = lista.get(0).getString(0);    
		String path = "Archivos/"+site+"/"+date.getMonth().getValue()+"/"+date.getYear()+"/"+date.getDayOfMonth()+"/";
		
		assertTrue(ManejoArchivos.existeDirectorio(path+ "/Positivo"));
	}
	
	@Test
	void test_5_desafio3_guardadoArchivosCeroCorrecto() {
		LocalDate date= LocalDate.now();
		Dataset<Row> response = Desafio3.ejecutarDesafio3();
		List<Row> lista = response.select("siteId").collectAsList();
	    String site = lista.get(0).getString(0);    
		String path = "Archivos/"+site+"/"+date.getMonth().getValue()+"/"+date.getYear()+"/"+date.getDayOfMonth()+"/";
		
		assertTrue(ManejoArchivos.existeDirectorio(path+ "/Cero"));
	}
	
	@Test
	void test_6_desafio3_guardadoArchivosNegativoCorrecto() {
		LocalDate date= LocalDate.now();
		Dataset<Row> response = Desafio3.ejecutarDesafio3();
		
		List<Row> lista = response.select("siteId").collectAsList();
	    String site = lista.get(0).getString(0);    
		String path = "Archivos/"+site+"/"+date.getMonth().getValue()+"/"+date.getYear()+"/"+date.getDayOfMonth()+"/";
		
		assertTrue(ManejoArchivos.existeDirectorio(path+ "/Negativo"));
	}
	
	@Test
	void test_7_desafio4_deserealizacionArrayCorrecta() {
		Dataset<Row> response = Desafio4.ejecutarDesafio4();
        StructType schema = new StructType()
        		.add("rowId","long",false)
        		.add("itemId","string",true)
        		.add("soldQuantity","long",true)
        		.add("availableQuantity","long",true);
        response.printSchema();
        response.show();
        assertEquals(schema,response.schema());
	}
	
	@Test
	void test_8_desafio5_deserealizacionCorrecta() {
		Dataset<Row> response = Desafio5.ejecutarDesafio5();
        StructType schema = new StructType()
        		.add("itemId","string",true)
        		.add("soldQuantity","long",true)
        		.add("visits","string",true);
        response.printSchema();
        response.show();
        assertEquals(schema,response.schema());
	}
	
	@Test
	void test_9_desafio5_VerificacionElementosSinVentasCorrecta() {
		Dataset<Row> response = Desafio5.ejecutarDesafio5();
		long sinVentas = response.filter("soldQuantity = 0").count();
        response.printSchema();
        response.show();
        assertEquals(0,sinVentas);
	}
	
	@Test
	void test_10_desafio6_deserealizacionCorrecta() {
		Dataset<Row> response = Desafio6.ejecutarDesafio6();
        StructType schema = new StructType()
        		.add("itemId","string",true)
        		.add("soldQuantity","long",true)
        		.add("visits","string",true)
        		.add("conversionRate","double",true)
        		.add("conversionRanking","integer",true);
        response.printSchema();
        response.show();
        assertEquals(schema,response.schema());
	}
	
	@Test
	void test_11_desafio6_mejorConversionRateCorrecta() {
		Dataset<Row> response = Desafio6.ejecutarDesafio6();
        List<Row> lista = response.select("conversionRate").collectAsList();
        response.printSchema();
        response.show();
        assertEquals(0.45454545454545453,lista.get(0).getDouble(0));
	}
	
	@Test
	void test_12_desafio7_availableQuantityOrdenadoDesc() {
		Dataset response = Desafio7.ejecutarDesafio7();
		response.show();
		List listavailableQuantity = response.select("availableQuantity").collectAsList();
		assertEquals("[999]", listavailableQuantity.get(0).toString());
	}
	
	@Test
	void test_13_desafio7_stockPorcentageCorrecto() {
		Dataset response = Desafio7.ejecutarDesafio7();
		response.show();
		List listaPorcentages = response.select("stockPercentage").collectAsList();
		assertEquals("[70.30]", listaPorcentages.get(0).toString());
	}

}
