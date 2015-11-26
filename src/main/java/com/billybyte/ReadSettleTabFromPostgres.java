package com.billybyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import com.billybyte.commonstaticmethods.CollectionsStaticMethods;

public class ReadSettleTabFromPostgres {

	public static void main(String[] args) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

    
        String url = "jdbc:postgresql://localhost/testdb";
        try{
//          con = DriverManager.getConnection(url, user, password);
          con = DriverManager.getConnection(url);
          st = con.createStatement();
          rs = st.executeQuery("SELECT VERSION()");

          if (rs.next()) {
              System.out.println(rs.getString(1));
          }

          String readSql = 
          		"Select * from settletab where " +
          		"shortname like 'CL.FUT%';" ;

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
        	  CollectionsStaticMethods.prtMapItems(map);

			} catch (Exception e) {
				e.printStackTrace();
			}
          
    	  st.close();

        }catch(Exception e){
        	
        }
	}
}
