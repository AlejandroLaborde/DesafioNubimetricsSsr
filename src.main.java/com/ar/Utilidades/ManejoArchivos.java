package com.ar.Utilidades;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ManejoArchivos {
	
	public static boolean crearCarpeta( String path , String nombreCarpeta ) {
		  File file = new File(path + "/"+ nombreCarpeta);
		  if(!file.isDirectory()) {
			  return file.mkdir();			  
		  }
		  return true;
	}	
	
	public static boolean existeArchivo( String path ) {
		  File file = new File(path);
		  return file.exists();
	}	
	
	public static boolean existeDirectorio( String path ) {
		  File file = new File(path);
		  return file.isDirectory();
	}	
	
	public static void escribirArchivoJson( String path, String data){
	    BufferedWriter bw = null;
        FileWriter fw = null;

        try {

            fw = new FileWriter(path);
            bw = new BufferedWriter(fw);
            bw.write(data);

        } catch (IOException e) {
        	e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
               ex.printStackTrace();
            }
        }
	}
	
	public static String leerArchivoJson( String path){
	    BufferedReader br = null;
        FileReader fr = null;
        StringBuilder sb = new StringBuilder();
        try {

            fr = new FileReader(path);
            br = new BufferedReader(fr);
            String lineaActual;
			while ((lineaActual = br.readLine()) != null) {
            	sb.append(lineaActual);
            }

        } catch (IOException e) {
        	e.printStackTrace();
        } finally {
            try {
                if (br != null)
                	br.close();

                if (br != null)
                	br.close();
            } catch (IOException ex) {
               ex.printStackTrace();
            }
        }
        return sb.toString();
	}
	
}
