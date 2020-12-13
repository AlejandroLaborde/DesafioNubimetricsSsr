package com.ar.Clases;


import java.time.LocalDate;
import com.ar.Excepciones.ErrorConexionException;
import com.ar.Servicios.ConexionesHttp;
import com.ar.Utilidades.ManejoArchivos;

public class Desafio2 {
	
	public static void ejecutarDesafio2( String pathCarpeta, String site, String categoria ) throws ErrorConexionException {
	
		LocalDate date = LocalDate.now();
		String nombreCarpeta = "search" + "json" + date.getYear() + date.getMonth().getValue();
		//Realizo conexion 
		ConexionesHttp conexionhttp= new ConexionesHttp();
		String respuesta = conexionhttp.getSearchCategory(site, categoria);
		if( ManejoArchivos.crearCarpeta(pathCarpeta, nombreCarpeta) ) {
			ManejoArchivos.escribirArchivoJson(pathCarpeta+"/"+ nombreCarpeta+ "/response.json", respuesta);	
		}
	}

	
}
