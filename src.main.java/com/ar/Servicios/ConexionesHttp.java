package com.ar.Servicios;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.ar.Excepciones.ErrorConexionException;

public class ConexionesHttp {
	
	public String getSearchCategory ( String site, String category ) throws ErrorConexionException {
		
				
		URL url = null;
		HttpURLConnection conn=null;
		InputStream rs = null;
		ByteArrayOutputStream bos=null;
		try {
			//Realizo llamada a la api
			url = new URL("https://api.mercadolibre.com/sites/"+ site +"/search?category="+ category );
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestProperty("accept", "application/json");
			rs = conn.getInputStream();
			bos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int length;
			if( conn.getResponseCode() == 200 ) {
				while ((length = rs.read(buffer)) != -1) {
					bos.write(buffer, 0, length);
				}
			}else {
				return null;
			}
			

			
		} catch (Exception e) {
			
			 throw new ErrorConexionException("Fallo al querer realizar la conexion");
		}finally {
			if(conn != null) {
				conn.disconnect();				
			}
			try {
				if( bos != null ) {
					bos.close();
				}
				
				if( rs!= null) {
					rs.close();
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		return bos.toString();
	}
	
}
