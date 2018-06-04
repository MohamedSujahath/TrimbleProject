package com.trimble.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;

import com.trimble.dataobjects.PersonalDetailsMySQL;

public class LoadingConsumer implements Runnable{

	private BlockingQueue<ArrayList<PersonalDetailsMySQL>> queue;
	private Connection conn;
    
    public LoadingConsumer(BlockingQueue<ArrayList<PersonalDetailsMySQL>> q, Connection conn){
        this.queue=q;
        this.conn = conn;
    }

    @Override
    public void run() {
    	
    	
    		
    	
        try{
           
            
            try {
    			conn.setCatalog("DataDB");
    		} catch (SQLException e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
        	String query = "insert into PersonalDetails (ID, Name, Telephone, Address,Age)"
        	        + " values (?, ?, ?, ?,?)";

        	ArrayList<PersonalDetailsMySQL> personalDetailsMySQLList = queue.take();
        	
        	System.out.println("Queue Size is :" + queue.size());
        	
        	for (PersonalDetailsMySQL personalDetailsMySQL : personalDetailsMySQLList) {
        		
        		PreparedStatement preparedStmt;
    			try {
    				preparedStmt = conn.prepareStatement(query);
    			
    		
      	      preparedStmt.setString(1, personalDetailsMySQL.getId());
      	      preparedStmt.setString(2, personalDetailsMySQL.getName());
      	      preparedStmt.setString(3, personalDetailsMySQL.getTelephone());
      	      preparedStmt.setString(4, personalDetailsMySQL.getAddress());
      	      preparedStmt.setString(5, personalDetailsMySQL.getAge());

      	      
      	      preparedStmt.execute();
      	      
    			} catch (SQLException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    		
            Thread.sleep(100);
            System.out.println("Consumed and Loading transformed List of 500 records into MySQL DB");
            
            }
        }catch(InterruptedException e) {
            e.printStackTrace();
        }
    	
    }
}
