const fs = require('fs');
var express = require('express');
var http = require('http');
var Promise = require('bluebird');
var RethinkdbWebsocketServer = require('rethinkdb-websocket-server');
var r = RethinkdbWebsocketServer.r;
var RP = RethinkdbWebsocketServer.RP;

var options = JSON.parse(fs.readFileSync('./config.js', 'utf8'));

var rethinkConn = Promise.promisify(r.connect)({
  host: options.dbHost,
  port: options.dbPort,
  db: 'test',
});
function runQuery(query) {
  return rethinkConn.then(function(conn) {
    return query.run(conn);
  });
}

options.sessionCreator = function(urlQueryParams) {
  var userQuery = r.table('users').get(urlQueryParams.userId);
  return runQuery(userQuery).then(function(user) {
    if (user && user.authToken === urlQueryParams.authToken) {
      return {curHerdId: user.herdId};
    } else {
      return Promise.reject('Invalid auth token');
    }
  });
};

options.queryWhitelist = [
  // r.table('turtles').filter({herdId: curHerdId})
  r.table('turtles')
   .filter({"herdId": RP.ref('herdId')})
   .opt("db", r.db("test"))
   .validate(function(refs, session) {
     return session.curHerdId === refs.herdId;
   }),

  // r.table('turtles').insert({herdId: 'alpha-squadron', name: 'Speedy'})
  r.table('turtles')
   .insert({
     "herdId": RP.ref('herdId'),
     "name": RP.check(function(actual, refs, session) {
       return typeof actual === 'string' && actual.trim();
     }),
   })
   .opt("db", r.db("test"))
   .validate(function(refs) {
     var herdId = refs.herdId;
     if (typeof herdId !== 'string') return false;
     var validHerdQuery = r.table('herds').get(herdId).ne(null);
     return runQuery(validHerdQuery);
   }),
];

var app = express();
app.use('/', express.static('assets'));
var httpServer = http.createServer(app);
options.httpServer = httpServer;
options.httpPath = '/rethinkApi';

RethinkdbWebsocketServer.listen(options);
httpServer.listen(8000);
