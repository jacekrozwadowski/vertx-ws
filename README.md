# Vert.x microservice with JDBC and SQL.

Project shows how to create Vert.x microservice with JDBC/SQL support.

Main features
- VxMicroservice callback version/style service
- VxMicroserviceRx reactive programming model using RxJava API 


# How to install/run

To run project you can use maven command:
```
mvn vertx:run
```
or run directly from IDE by as Java Application


# How to use

Below curl commands show how to get range data from web service:

``` 
curl --user mpv:mpv -X GET http://localhost:8083/getRange/120041667

``` 
