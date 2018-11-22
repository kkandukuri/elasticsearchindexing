package com.elasticsearch.resources;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.elasticsearch.config.ElasticSearchConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;

import io.swagger.annotations.ApiOperation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

@RestController
public class ElasticSearchResource {
	
	private static final Logger logger = LogManager.getLogger(ElasticSearchResource.class);

	@Autowired
	private ElasticSearchConfig config;
	
	
	
	
	@ApiOperation(value = "Load All Offers")
	@RequestMapping(value = "/elasticsearch", method = RequestMethod.GET, produces = "application/json")
	public String loadOffersFromMirakel1() {
		
		RestTemplate restTemplate=new RestTemplate();
		HttpHeaders header=new HttpHeaders();
		header.set("content-type", "application/json");
		HttpEntity<String> request = new HttpEntity<String>(header);
		ResponseEntity<String> products;
		ResponseEntity<String> catalog;
		ResponseEntity<String> manufacturers;
		try {
			products = restTemplate.exchange("http://132.148.147.8:85/apis/Product/ProductsByTag?id=health", HttpMethod.GET, request, String.class);
			catalog=restTemplate.exchange("http://132.148.147.8:85/apis/Catalog/GetAllCategories/1", HttpMethod.GET, request, String.class);
			manufacturers=restTemplate.exchange("http://132.148.147.8:85/apis/Catalog/GetAllManufacturer/1", HttpMethod.GET, request, String.class);
		    try {
		    	//logger.info(response.getBody());
				String responseBody=products.getBody();
				ObjectMapper mapper = new ObjectMapper();
			    JsonNode actualObj = mapper.readTree(responseBody);
			    //logger.info(actualObj.get("items"));
			    JsonNode data=actualObj.get("items");
			    logger.info(data.get(0));
			    
		    	logger.info("Getting transport client");
	            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("132.148.131.148"), 9300));
	            logger.info("Got transport client");
	            int i=0;
			   while(true) {
				   if(data.get(i)!=null) {
					   try {
				    		
				    	IndexResponse response11 = client.prepareIndex("products", "doc").setSource(data.get(i++).toString(), XContentType.JSON).get();
		                String _index = response11.getIndex();
		                String _type = response11.getType();
		                String _id = response11.getId();
		                long _version = response11.getVersion();
		                logger.info(">>>>>>>>>>>"+_id+"Processed Total products:"+i);
				    	}catch(Exception e) {
				    		//e.printStackTrace();
				    		break;
				    	}
			   		}else {
			   			break;
			   		}
			    }
			   logger.info("Total products loaded: "+i);
			    	client.close();
			    }catch(Exception e) {
			    	e.printStackTrace();
		    }
		    logger.info("Loaded products.... Starting catalog...");
		    try {
		    	logger.info(catalog.getBody());
				String responseBody=catalog.getBody();
				ObjectMapper mapper = new ObjectMapper();
			    JsonNode data = mapper.readTree(responseBody);
		    	logger.info("Getting transport client");
	            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
	            logger.info("Got transport client");
	            int i=0;
			   while(true) {
				   if(data.get(i)!=null) {
					   try {
				    		
				    	IndexResponse response11 = client.prepareIndex("catalog", "doc").setSource(data.get(i++).toString(), XContentType.JSON).get();
		                String _index = response11.getIndex();
		                String _type = response11.getType();
		                String _id = response11.getId();
		                long _version = response11.getVersion();
		                logger.info(">>>>>>>>>>>"+_id+"Processed Total products:"+i);
				    	}catch(Exception e) {
				    		//e.printStackTrace();
				    		break;
				    	}
			   		}else {
			   			break;
			   		}
			    }
			   logger.info("Total products loaded: "+i);
			    	client.close();
			    }catch(Exception e) {
			    	e.printStackTrace();
		    }
		    logger.info("Loaded catalog.... Starting manufacturers...");
		    try {
		    	//logger.info(response.getBody());
				String responseBody=manufacturers.getBody();
				ObjectMapper mapper = new ObjectMapper();
			    JsonNode data = mapper.readTree(responseBody);
		    	logger.info("Getting transport client");
	            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
	            logger.info("Got transport client");
	            int i=0;
			   while(true) {
				   if(data.get(i)!=null) {
					   try {
				    		
				    	IndexResponse response11 = client.prepareIndex("manufacturers", "doc").setSource(data.get(i++).toString(), XContentType.JSON).get();
		                String _index = response11.getIndex();
		                String _type = response11.getType();
		                String _id = response11.getId();
		                long _version = response11.getVersion();
		                logger.info(">>>>>>>>>>>"+_id+"Processed Total products:"+i);
				    	}catch(Exception e) {
				    		//e.printStackTrace();
				    		break;
				    	}
			   		}else {
			   			break;
			   		}
			    }
			   logger.info("Total products loaded: "+i);
			    	client.close();
			    }catch(Exception e) {
			    	e.printStackTrace();
		    }
//		    try {
//	            // create client for localhost es
//	            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
//	                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
//
//	            // iterate through json files, indexing each
//	            for (int n = 0; n < files.length; n++) {
//	                
//	                IndexResponse response = client.prepareIndex("recipes", "doc").setSource(json, XContentType.JSON).get();
//	                String _index = response.getIndex();
//	                String _type = response.getType();
//	                String _id = response.getId();
//	                long _version = response.getVersion();
//	            }
//
//	            // close es client
//	            client.close();
//	        } catch (IOException e) {
//	            e.printStackTrace();
//	        }
			//List<OfferData> savedOffer = offerRepository.saveAll(offerDataList);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "sucess";
	}
//	@ApiOperation(value = "Get All Offers")
//	@RequestMapping(value = "/offers", method = RequestMethod.GET, produces = "application/json")
//	public List<OfferData> retrieveAllOffers() {
//		return offerRepository.findAll();
//	}
	
	
	
}
