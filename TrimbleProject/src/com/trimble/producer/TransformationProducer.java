package com.trimble.producer;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;

import com.trimble.dataobjects.PersonalDetails;
import com.trimble.dataobjects.PersonalDetailsMySQL;

public class TransformationProducer implements Runnable{
	

	   private BlockingQueue<ArrayList<PersonalDetailsMySQL>> queue;
	   
	   private ArrayList<PersonalDetails> mongoDbList;
	    
	    public TransformationProducer(BlockingQueue<ArrayList<PersonalDetailsMySQL>> q, ArrayList<PersonalDetails> mongoDbList){
	        this.queue=q;
	        this.mongoDbList = mongoDbList;
	    }
	    
	    @Override
	    public void run() {
	       
	        
	          
				
			
	            
	            ArrayList<PersonalDetailsMySQL> transformedList = new ArrayList<PersonalDetailsMySQL>();
	            int count = 0;
	            ListIterator<PersonalDetails> iter = mongoDbList.listIterator();
	            ArrayList<PersonalDetails> removeMongoDBList = new ArrayList<PersonalDetails>();
	           
	            while(iter.hasNext()){
	            	PersonalDetailsMySQL pdSQL = new PersonalDetailsMySQL();
	            	PersonalDetails personalDetails  = iter.next();
	            	pdSQL.setId(personalDetails.getId());
	            	pdSQL.setName(personalDetails.getName());
	            	pdSQL.setAddress(personalDetails.getAddress());
	            	pdSQL.setTelephone(personalDetails.getTelephone());
	            	pdSQL.setAge(personalDetails.getAge());
	            	transformedList.add(pdSQL);
	            	removeMongoDBList.add(personalDetails);
	            	count++;
	            	if(count == 500)
	            	{
	            		break;
	            	}
				}
	            
	            mongoDbList.removeAll(removeMongoDBList);
	            removeMongoDBList.clear();
	            
	            try {
	                Thread.sleep(100);
	                queue.put(transformedList);
	                System.out.println("Produced transformed List of 500 records + Queue Size:" + queue.size());
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
	            
	           
	        
	       
	    }

	

}
