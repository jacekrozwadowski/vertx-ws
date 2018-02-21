package vertx.ws;

import vertx.ws.util.Runner;

import io.vertx.reactivex.ext.auth.shiro.ShiroAuth;
import io.vertx.reactivex.ext.auth.AuthProvider;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.handler.AuthHandler;
import io.vertx.reactivex.ext.web.handler.BasicAuthHandler;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.sql.SQLConnection;
import io.vertx.reactivex.ext.sql.SQLRowStream;
import io.reactivex.Single;
import io.vertx.core.http.HttpHeaders;
import io.vertx.reactivex.core.AbstractVerticle;

import io.vertx.ext.auth.shiro.ShiroAuthRealmType;


public class VxMicroserviceRx extends AbstractVerticle {
	
	// Convenience method so you can run it in your IDE
	public static void main(String[] args) {
		Runner.runExample(VxMicroserviceRx.class);
	}
	
	JDBCClient client;
	
	String query = "SELECT item, loc, loc_type, unit_retail, promo_retail, status from item_loc WHERE item = ?";
	
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
		        .put("password", "<pass>"), "MyDS");
		
		Router router = Router.router(vertx);
		router.route("/getRange/*").handler(basicAuthHandler);
		router.get("/getRange/:item").handler(this::getRange);	
		
		vertx.createHttpServer()
		.requestHandler(router::accept)
		.listen(8083);
	}
	
	private void getRange(RoutingContext rc) {
		
		String qItem = rc.pathParam("item")==null? "" : rc.pathParam("item");
		
		client.rxGetConnection().flatMap(sqlConnection -> {
			Single<JsonArray> ts = findItemLoc(sqlConnection, qItem);
			
			return ts.doAfterTerminate(sqlConnection::close);
		}).subscribe(
			list -> {
				rc.response()
				.putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
				.end(list.encode());
			}, err -> {
				rc.fail(503);
				err.printStackTrace();
			}
		);
		
	}
	
	private Single<JsonArray> findItemLoc(SQLConnection sqlConnection, String item) {
		return sqlConnection.rxQueryStreamWithParams(query, new JsonArray().add(item))
				.flatMapObservable(SQLRowStream::toObservable)
				.map(this::getRangeObject)
				.collect(JsonArray::new, JsonArray::add);
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
