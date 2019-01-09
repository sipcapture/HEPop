/*
 * Bulk Bucket Emitters
 * bucket.push(object);
 */

var config = require('./config').getConfig();
const log = require('./logger');
const bucket_emitter = require('./bulk-emitter')
const stringify = require('safe-stable-stringify');
var r = false;
var pgp = false;

var r_bucket;
var p_bucket;
var e_bucket;
var m_bucket;
var l_bucket;

// RethinkDB
if (config.db.rethink){
 try {
  r = require('./rethink').connect();
  log('%start:green Initialize RethinkDB driver' );

  log('%start:green Initializing Bulk bucket...');
  r_bucket = bucket_emitter.create(config.queue);
  r_bucket.on('data', function(data) {
    // Bulk ready to emit!
    if (config.debug) log('%data:cyan RethinkDB BULK Out %s:blue', stringify(data) );
    r.db(config.dbName).table(config.tableName).insert(data).run(durability="soft", noreply=true);
  }).on('error', function(err) {
    log('%error:red %s', err.toString() )
  });

 } catch(e){ log('%stop:red Failed to Initialize RethinkDB driver/queue',e); return; }
}

exports.bucket = r_bucket;
exports.r = r;

// PGSql
if (config.db.pgsql){
 try {
  pgp = require('./pgsql');
  pgp.setTemplate(config.db.pgsql.schema);
  p_bucket = bucket_emitter.create(config.queue);
  p_bucket.on('data', function(data,id) {
    // Bulk ready to emit!
    if (config.debug) log('%data:cyan PGSQL BULK ID %s:blue', id );
    if (config.debug) log('%data:cyan PGSQL BULK Out %s:blue', stringify(data) );
    pgp.insert(data,id);
  }).on('error', function(err) {
    log('%error:red %s', err.toString() )
  });

 } catch(e){ log('%stop:red Failed to Initialize PGSql driver/queue',e); return; }
}

exports.pgp_bucket = p_bucket;
exports.pgp = pgp;

// MongoDB
if (config.db.mongodb && config.db.mongodb.url){
 try {
  m_bucket = bucket_emitter.create(config.queue);
  mongo = require('./mongodb');
  var error = mongo.connectDB(function(err){
	if (err) throw err;
	var mdb = mongo.getDB();

	  m_bucket.on('data', function(data) {
	    // Bulk ready to emit!
	    if (config.debug) log('%data:cyan MongoDB BULK Out %s:blue', stringify(data) );
	    	mdb.collection(config.tableName).insertMany(data, function(err,result){
			if (err) log('%stop:red Failed to Insert: ',err);
			if (config.debug) log('%start:green Inserted: ', result.insertedCount);
		});
	  }).on('error', function(err) {
	    log('%error:red %s', err.toString() )
	  });

  });

 } catch(e){ log('%stop:red Failed to Initialize MongoDB driver/queue',e); return; }
}

exports.mdb_bucket = m_bucket;


// Elastic
if (config.db.elastic){
 try {
  var EsBroker = require('./es-broker');
  e_bucket = EsBroker.create(config.db.elastic);
  e_bucket.set_id = function(id){ return; }
 } catch(e){ log('%stop:red Failed to Initialize Elastic Bulk driver/queue',e); return; }

}

exports.e_bucket = e_bucket;


// Loki
if (config.db.loki){
 try {
  var loki = require('./loki');
  log('%start:green Initialize Loki driver' );
  log('%start:green Initializing Bulk bucket...');
  l_bucket = bucket_emitter.create(config.queue);
  l_bucket.set_id = function(id){ return; }
  l_bucket.on('data', function(data) {
    // Bulk ready to emit!
    if (config.debug) log('%data:cyan Loki BULK Out %s:blue', stringify(data) );
		loki.insert(data);
  }).on('error', function(err) {
    log('%error:red %s', err.toString() )
  });

 } catch(e){ log('%stop:red Failed to Initialize Loki driver/queue',e); return; }
}

exports.l_bucket = l_bucket;


process.on('beforeExit', function() {
  bucket.close(function(leftData) {
    if (config.debug) log('%data:red BULK Leftover [%s:blue]', stringify(leftData) );
    if (r) r.db(config.dbName).table(config.tableName).insert(leftData).run(durability="soft", noreply=True);
  });
});

