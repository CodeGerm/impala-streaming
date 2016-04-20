package org.cg.impala.streaming.server;

import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;

public class Responses {
	
	public static HttpResponse NotFoundResponse(String message){
		HttpResponse response = HttpResponse.create()
	            .withEntity(message)
	            .withStatus(StatusCodes.NOT_FOUND);
		
		return response;
	}
	
	public static HttpResponse NotFoundResponse(Exception e){	
		return NotFoundResponse(e.getMessage());
	}
	
	public static HttpResponse InternalErrorResponse(String message){
		HttpResponse response = HttpResponse.create()
	            .withEntity(message)
	            .withStatus(StatusCodes.INTERNAL_SERVER_ERROR);
		return response;
	}
	
	public static HttpResponse InternalErrorResponse(Exception e){
		return InternalErrorResponse(e.getMessage());
	}
	
	public static HttpResponse BadRequestResponse(String message){
		HttpResponse response = HttpResponse.create()
	            .withEntity(message)
	            .withStatus(StatusCodes.BAD_REQUEST);
		return response;
	}
	
	public static HttpResponse BadRequestResponse(Exception e){
		return BadRequestResponse(e.getMessage());
	}
	
	
	public static HttpResponse successResponse(String message){
		HttpResponse response = HttpResponse.create()
	            .withEntity(message)
	            .withStatus(StatusCodes.OK);
		return response;
	}
	
	public static void main(String args[]){
		HttpResponse response = Responses.successResponse("gogogo");
		System.out.println(response);
		
	}

}
