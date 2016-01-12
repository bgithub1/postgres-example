package com.billybyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;


public class ReadPortfolio {
	public static void main(String[] args) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

    
        String url = "jdbc:postgresql://amazon_url";
        url = url.replace("amazon_url", args[0]);
        String username = args[1];
        String pass = args[2];
        try {
			con = DriverManager.getConnection(url,username,pass);
	        st = con.createStatement();
	        rs = st.executeQuery("SELECT VERSION()");

	        if (rs.next()) {
	            System.out.println(rs.getString(1));
	        }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        String readSql = 
          		"select id,syscountyname, parcel1, certyear, taxyear, ownername from portfolios.fl_taxcertpurchases where syscountyname='DUVAL'" ;

          try {
        	  rs  = st.executeQuery(readSql);
        	  Map<String,String> map = new TreeMap<String, String>();
        	  while (rs.next())
        	  {
				ResultSetMetaData rsmd = rs.getMetaData();
				int numCols = rsmd.getColumnCount();
				String s="";
				String sn = rs.getString(1);
				for(int i=1;i<=numCols;i++){
       			  s+=rs.getString(i)+",";
				}
				map.put(sn,s.substring(0,s.length()-1));
        	  } 
        	  rs.close();
        	  for(Entry<String, String> entry : map.entrySet()){
        		  System.out.println(entry.getValue());
        	  }

			} catch (Exception e) {
				e.printStackTrace();
			}
        

	}
}
