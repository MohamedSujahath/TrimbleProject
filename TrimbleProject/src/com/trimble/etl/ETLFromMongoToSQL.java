package com.trimble.etl;


import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.trimble.consumer.LoadingConsumer;
import com.trimble.dataobjects.PersonalDetails;
import com.trimble.dataobjects.PersonalDetailsMySQL;
import com.trimble.producer.TransformationProducer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;


public class ETLFromMongoToSQL {
	
	private static String jdbcDriver = "com.mysql.jdbc.Driver";
    private static String dbName = "DataDB";
    
    Connection conn;
    
    ArrayList<PersonalDetailsMySQL> mysqlDBDataList = new ArrayList<PersonalDetailsMySQL>();
	
	private ArrayList<PersonalDetails> extractOperation()
	{
		
		ArrayList<PersonalDetails> mongoDBDataList = new ArrayList<PersonalDetails>();
		try {
            
            MongoClient mongoClient = new MongoClient("localhost", 27017);
          
          
          MongoDatabase database = mongoClient.getDatabase("DataDB");
  		  MongoCollection<Document> collection = database
  				.getCollection("PersonalDetails");

	  		List<Document> personalDetails = (List<Document>) collection.find().into(
	  				new ArrayList<Document>());
	
	                 for(Document detail : personalDetails){
	                     
	                     PersonalDetails pd = new PersonalDetails();
	                     pd.setUid(detail.get("_id").toString());
	                     pd.setId(detail.get("ID").toString());
	                     pd.setName(detail.get("Name").toString());
	                     pd.setTelephone(detail.get("Telephone").toString());
	                     pd.setAddress(detail.get("Address").toString());
	                     pd.setAge(detail.get("Age").toString());
	                     mongoDBDataList.add(pd);
	                 }
             
	                 mongoClient.close();
             
        }
		finally{
			
		}
		
		return mongoDBDataList;
	}
	
	
	
    
    private void connectToMySQL()
    {
    	try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	String url = "jdbc:mysql://localhost:3306/";

        
        String username = "root";
        String password = "root";

        // SQL command to create a database in MySQL.
        String sql = "CREATE DATABASE IF NOT EXISTS DataDB";

        try{
        conn = DriverManager.getConnection(url, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql);

             stmt.execute();
             
             String sql_stmt = "CREATE TABLE IF NOT EXISTS DataDB.`PersonalDetails` (\n"
                     + "    `Uid` INTEGER(5) UNSIGNED NOT NULL AUTO_INCREMENT,\n"
                     + "    `ID` VARCHAR(45) NOT NULL,\n"
                     + "    `Name` VARCHAR(100) NOT NULL,\n"
                     + "    `Telephone` VARCHAR(45) DEFAULT NULL,\n"
                     + "    `Address` VARCHAR(200) DEFAULT NULL,\n"
                     + "    `Age` VARCHAR(45) DEFAULT NULL,\n"
                     + "    PRIMARY KEY (`Uid`)\n"
                     + ");";

             Statement statement = conn.createStatement();

             statement.executeUpdate(sql_stmt);
        } catch (Exception e) {
            e.printStackTrace();
        }
    	
    }
    
    private void startProducerConsumerBlkQueue(ArrayList<PersonalDetails> mongoDBDataList)
    {
    	// Each producer transforms upto 500 records. You can try increasing the number of Producers or increase the number of records each producer processes to cover up more rows.
    	int noOfProducers = 300;
    	int noOfConsumers = 280;
         
        BlockingQueue<ArrayList<PersonalDetailsMySQL>> myQueue = new LinkedBlockingQueue<ArrayList<PersonalDetailsMySQL>>(20);
        System.out.println("My Queue size:" + myQueue.size());
        
        
        for(int i =0 ; i < noOfProducers ; i++){
            new Thread(new TransformationProducer(myQueue,mongoDBDataList)).start();
        }
       
  
        for(int i =0 ; i < noOfConsumers ; i++){
        	new Thread(new LoadingConsumer(myQueue, conn)).start();
        }
       
      
        try {
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ETLFromMongoToSQL etl = new ETLFromMongoToSQL();
		
		ArrayList<PersonalDetails> mongoDBDataList = new ArrayList<PersonalDetails>();
		
		mongoDBDataList = etl.extractOperation();
		
		
		etl.connectToMySQL();
		
		etl.startProducerConsumerBlkQueue(mongoDBDataList);
		

	}

}
