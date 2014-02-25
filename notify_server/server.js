/**
 * Created by kost on 24.02.14.
 */

var express = require('express')
var app = express()
var sockjs  = require('sockjs');
var http    = require('http');
var redis = require('redis');
var stringify = require('json-stringify-safe');

app.all('*', function(req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'PUT, GET, POST, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
});

app.use(function(req, res, next){
    console.log('%s %s', req.method, req.url);
    next();
});

app.get('/ping', function(res, req){
    req.send('pong');
});

// 1. Echo sockjs server
var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};

var sockjs_echo = sockjs.createServer(sockjs_opts);

sockjs_echo.on('connection', function(conn) {
    var subs = redis.createClient()

    conn.on('data', function(raw) {
        var data = JSON.parse(raw)

        console.log("data: " + stringify(data));
        if(data.type === "init"){
            subs.subscribe(data.content);
            console.log("Subscribed to " + data.content );
        }
        if(data["type"] === "msg"){
            conn.write(data["content"]);
        };
    });

    subs.on("message", function(ch, msg){
        conn.write(msg);
        console.log(msg);
    });

});

// 2. Express server
var server = http.createServer(app);

sockjs_echo.installHandlers(server, {prefix:'/subscribe'});

console.log(' [*] Listening on 0.0.0.0:9999' );
server.listen(9999, '0.0.0.0');
