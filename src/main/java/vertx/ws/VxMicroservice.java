package vertx.ws;

import vertx.ws.util.Runner;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.shiro.ShiroAuth;
import io.vertx.ext.auth.shiro.ShiroAuthRealmType;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.BasicAuthHandler;


public class VxMicroservice extends AbstractVerticle {
	
	//Convenience method so you can run it in your IDE
	public static void main(String[] args) {
		Runner.runExample(VxMicroservice.class);
	}
	
	JDBCClient client;
	
	@Override
	public void start() {
		
		JsonObject config = new JsonObject().put("properties_path", "classpath:test-auth.properties");

		AuthProvider authProvider = ShiroAuth.create(vertx, ShiroAuthRealmType.PROPERTIES, config);
		AuthHandler basicAuthHandler = BasicAuthHandler.create(authProvider);
		
		client = JDBCClient.createShared(vertx, new JsonObject()
		        .put("url", "jdbc:oracle:thin:@<hostname>:1521:<SID>")
		        .put("driver_class", "oracle.jdbc.driver.OracleDriver")
		        .put("max_pool_size", 10)
		        .put("user", "<user>")
		        .put("password", "<pass>"));
		
		Router router = Router.router(vertx);
		router.route("/getRange/*").handler(basicAuthHandler);
		router.get("/getRange/:item").handler(this::getRange);
		
		vertx.createHttpServer()
		.requestHandler(router::accept)
		.listen(8083);
	}
	
	private void getRange(RoutingContext rc) {
		
		client.getConnection(conn -> {
			if (conn.failed()) {
				System.err.println(conn.cause().getMessage());
		        return;
		    }
			
			final SQLConnection connection = conn.result();
			
			String qItem = "";
			if (rc.pathParam("item") != null) {
				qItem = rc.pathParam("item");
			}
			
			String query = "SELECT item, loc, loc_type, unit_retail, promo_retail, status from item_loc WHERE item = ?";
			JsonArray params = new JsonArray().add(qItem);
			
			connection.queryWithParams(query, params, res -> {
				if (res.succeeded()) {
					// Get the result set
				    ResultSet resultSet = res.result();
				    
				    JsonArray list = new JsonArray();
				    
				    resultSet.getResults().stream().map(this::getRangeObject).forEach(list::add);
				      
				    rc.response()
					.putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
					.end(list.encode());
				    
				} else {
					// Failed!
					rc.fail(503);
					res.cause().printStackTrace();
				}
			});
			
		}); 
		
	}
	
	private JsonObject getRangeObject(JsonArray row) {
		String item = row.getString(0);
    	Integer location = row.getInteger(1);
    	String locType = row.getString(2);
    	Double sellingRetail = row.getDouble(3);
    	Double promoRetail = row.getDouble(4);
    	String status = row.getString(5);
    	
    	JsonObject json = new JsonObject()
    			.put("item", item)
    			.put("location", location)
    			.put("locType", locType)
    			.put("sellingRetail", sellingRetail)
    			.put("promoRetail", promoRetail)
    			.put("status", status);
    	
    	return json;
	}

}
