package com.criticalsoftware.filewatcher.webclient;

public class HttpResponse {
	private int statusCode;
	private String body;
	
	public HttpResponse() {
		super();
	}
	
	public HttpResponse(int statusCode, String messageBody) {
		super();
		this.statusCode = statusCode;
		this.body = messageBody;
	}
	
	public int getStatusCode() {
		return statusCode;
	}
	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
	public String getBody() {
		return body;
	}
	public void setBody(String body) {
		this.body = body;
	}
	
}
