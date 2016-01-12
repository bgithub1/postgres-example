package com.billybyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

import com.billybyte.commonstaticmethods.CollectionsStaticMethods;

public class ReadPortfolio {
	public static void main(String[] args) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

    
        String url = "jdbc:postgresql://test.cxpdwhygwwwh.us-east-1.rds.amazonaws.com/prod";
        try {
			con = DriverManager.getConnection(url,"billy","figtree77*");
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
        	  CollectionsStaticMethods.prtMapItems(map);

			} catch (Exception e) {
				e.printStackTrace();
			}
        

	}
}
