package com.billybyte;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.billybyte.commoninterfaces.SettlementDataInterface;
import com.billybyte.commonstaticmethods.Utils;
import com.billybyte.mongo.MongoXml;

/**
 * write settlement data from MongoXml<SettlementDataInterface> db to postgres settletab table
 * Must have both mongo and postgres servers running
 * @author bperlman1
 *
 */
public class WriteSettleXmlToPostgres {
	private final MongoXml<SettlementDataInterface> settledb;
	private final Statement st;

	
	
	public WriteSettleXmlToPostgres(
			MongoXml<SettlementDataInterface> settledb, Statement st) {
		super();
		this.settledb = settledb;
		this.st = st;
	}
	
	
	/**
	 * Method that loops through keys, fetching them from mongo and writing data to postgres
	 * @param batchNum
	 * @param keyList
	 */
	private final void writeBatch(Set<String> batchKeySet){
        Map<String, SettlementDataInterface> map = 
        		settledb.findFromSet(batchKeySet);
        for(Entry<String, SettlementDataInterface> entry: map.entrySet()){
/**
INSERT INTO table_name (column1,column2,column3,...)
VALUES (value1,value2,value3,...); 
*             	
*/
        	SettlementDataInterface settle = entry.getValue();
        	String shortName = settle.getShortName();
        	String price = settle.getPrice().toString();
        	String size = new Integer(settle.getSize()).toString();
        	String time = new Long(settle.getTime()).toString();
        	String insertSql = "INSERT INTO settletab (shortname,price,size,time)" +
        			"VALUES ('" + 
        				shortName + "','" + 
        				price + "','" + 
        				size + "','" + 
        				time + "'" + 
        			");" +
        			"";
            try {
				st.execute(insertSql);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
		
	}
    public static void main(String[] args) {

        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

    
        String url = "jdbc:postgresql://localhost/testdb";
//        String user = "user12";
//        String password = "34klq*";
//        String user = "bperlman1";
//        String password = "btunes";

        try {
//            con = DriverManager.getConnection(url, user, password);
            con = DriverManager.getConnection(url);
            st = con.createStatement();
            rs = st.executeQuery("SELECT VERSION()");

            if (rs.next()) {
                System.out.println(rs.getString(1));
            }
            String createTableSql = 
            		"create table settletab (" +
            		"shortName varchar(255), " +
            		"price float, " +
            		"size int, " +
            		"time int);" ;

            try {
				st.execute(createTableSql);
			} catch (Exception e) {
				e.printStackTrace();
			}
            
            // delete previous records
            String deleteRowsSql = 
            		"DELETE FROM settletab;" ;

            try {
				st.execute(deleteRowsSql);
			} catch (Exception e) {
				e.printStackTrace();
			}
            
            MongoXml<SettlementDataInterface> settledb = 
            		new MongoXml<SettlementDataInterface>(
            				"127.0.0.1", 27017, "settleDb", "settleColl");
            
            // create WriteSettleXmlToPostgres instance
            int batchSize=3000;
            WriteSettleXmlToPostgres wsxml = 
            		new WriteSettleXmlToPostgres(settledb, st);
            
            
            Set<String> keys =
            		settledb.findKeysFromRegex(".*");
            int numBatches = keys.size()/batchSize;
            List<String> keyList = new ArrayList<String>(keys);
            for(int i=0;i<numBatches;i++){
            	Utils.prtObMess(WriteSettleXmlToPostgres.class, "writing batch " + i);
            	Set<String> batchKeySet = new HashSet<String>();
            	for(int j=0;j<batchSize;j++){
            		batchKeySet.add(keyList.get(i*batchSize+j));
            	}
            	wsxml.writeBatch(batchKeySet);
//                Map<String, SettlementDataInterface> map = 
//                		settledb.findFromSet(batchKeySet);
//                for(Entry<String, SettlementDataInterface> entry: map.entrySet()){
//    /**
//    INSERT INTO table_name (column1,column2,column3,...)
//    VALUES (value1,value2,value3,...); 
//     *             	
//     */
//                	SettlementDataInterface settle = entry.getValue();
//                	String shortName = settle.getShortName();
//                	String price = settle.getPrice().toString();
//                	String size = new Integer(settle.getSize()).toString();
//                	String time = new Long(settle.getTime()).toString();
//                	String insertSql = "INSERT INTO settletab (shortname,price,size,time)" +
//                			"VALUES ('" + 
//                				shortName + "','" + 
//                				price + "','" + 
//                				size + "','" + 
//                				time + "'" + 
//                			");" +
//                			"";
//                    try {
//        				st.execute(insertSql);
//        			} catch (Exception e) {
//        				e.printStackTrace();
//        			}
//                }
            	
            }
            
            // calculate leftover data from last batch
            int totalRecsThusFar = batchSize * numBatches;
            int diff = keys.size()-totalRecsThusFar;
        	Utils.prtObMess(WriteSettleXmlToPostgres.class, "writing batch " + numBatches);
        	Set<String> batchKeySet = new HashSet<String>();
        	for(int j=0;j<diff;j++){
        		batchKeySet.add(keyList.get(totalRecsThusFar+j));
        	}
        	wsxml.writeBatch(batchKeySet);

            
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(WriteSettleXmlToPostgres.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(WriteSettleXmlToPostgres.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }
}