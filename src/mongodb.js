/*
 * ReThinkDB Connector
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var MongoClient = require('mongodb').MongoClient;

let _db

 const connectDB = async (callback) => {
     var rtconfig = require('./config').getConfig();
     if(!rtconfig.db.mongodb.url) {
       log('%stop:red Failed Initializing MongoDB [%s:blue]');
       process.exit();
       //return;
     }
     log('%start:cyan Initializing MongoDB [%s:blue]',stringify(rtconfig.db.mongodb.url));
     try {
         MongoClient.connect(rtconfig.db.mongodb.url, (err, db) => {
             _db = db.db(rtconfig.dbName);
             return callback(err)
         })
     } catch (e) {
         throw e
     }
 }

 const getDB = () => _db

 const disconnectDB = () => _db.close()

 module.exports = { connectDB, getDB, disconnectDB }
