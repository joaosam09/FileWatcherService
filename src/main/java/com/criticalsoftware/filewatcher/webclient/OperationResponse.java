package com.criticalsoftware.filewatcher.webclient;

/**
 * Class representing an operation response.
 * Contains result and date of the operation.
 *  
 * @author João Santos
 * @version 1.0
 */
public class OperationResponse {
	private double result;	
	private String time;
	
	public OperationResponse() {
		super();
	}
	
	public OperationResponse(double result, String time) {
		super();	
		this.result = result;
		this.time = time;
	}
	
	public double getResult() {
		return result;
	}
	
	public void setResult(double result) {
		this.result = result;
	}
	
	public String getTime() {
		return time;
	}
	
	public void setTime(String date) {
		this.time = date;
	}	
}
