/*
 * ReThinkDB Connector
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var mdb,table;

var MongoClient = require('mongodb').MongoClient
  , assert = require('assert');


exports.connect = function(){
  var rtconfig = require('./config').getConfig();
  if(!rtconfig.db.mongodb) {
    log('%stop:red Failed Initializing MongoDB [%s:blue]');
    process.exit();
    //return;
  }
  log('%start:cyan Initializing MongoDB [%s:blue]',stringify(rtconfig.db.mongodb.url));

  MongoClient.connect(rtconfig.db.mongodb.url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected successfully to server");
  mdb = db;
  //db.close();
  });

  exports.db = mdb;
  return mdb;
}

