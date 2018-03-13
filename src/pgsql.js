/*
 * HEPop PGsql connector
 *
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
const pgp = require('pg-promise')({
    /* initialization options */
    capSQL: true, // capitalize all generated SQL
    connect(client, dc, isFresh) {
        const cp = client.connectionParameters;
        log('%start:green Connected to PGsql database:', cp.database);
    },
    error(err, e) {
        log('%stop:red Caught PGsql error:', err,e);
   }
});
var config = require('./config').getConfig();
var cs;
var db;

var prepare = function(query){
	db.none(query)
	    .then(data => {
	        // success;
		return true;
	    })
	    .catch(error => {
	        // error;
		if (config.debug) log('ERR: %s',error);
		return false;
	    });
}

if(!config.db.pgsql) {
    log('%stop:red Missing configuration for PGsql [%s:blue]');
    process.exit();
    //return;
  }

try {
    db = pgp(config.db.pgsql);
    db.connect();
    exports.pgp = db;
    log('%start:cyan Initializing PGSql driver [%s:blue]', stringify(config.db.pgsql));
    var doDb = "CREATE DATABASE "+config.dbName;
    prepare(doDb);
    var doTable = "CREATE TABLE IF NOT EXISTS "+config.tableName+" (ID serial NOT NULL PRIMARY KEY, data json NOT NULL );"
    prepare(doTable);
} catch(err){
    log('%stop:red Failed Initializing PGsql driver [%s:blue]',err);
    process.exit();
    // return;
}


if (!config.db.pgsql.schema){
  var tableSchema = config.db.pgsql.schema || ['data'];
  var tableName = {table: config.tableName || 'hepic' };
  cs = new pgp.helpers.ColumnSet(tableSchema, tableName);
}

// PGSql connector;
// Column template to be generated and then shared/cached/reused for high performance
// Create JSON table if not existing
exports.setTemplate = function(name){
	if (!config.db.pgsql.schema||!name) return false;
	cs = new pgp.helpers.ColumnSet(schema, name);
	prepare(name);
};


// Generating a multi-row insert query:
// insert(values, cs);
// => INSERT INTO "hepic"("col_a","col_b") VALUES('a1','b1'),('a2','b2')
exports.insert = function(bulk){
	if (!cs) {
	    log('%stop:red Missing Schema/Templace for PGsql [%s:blue]');
	    return false;
	}
	//var query = pgp.helpers.insert(bulk,cs); // params: bulk_array, template
	var values = bulk.map(function(item) {
	  return "'"+stringify(item).replace(/,}/,"}")+"'";
	});
	var insertquery = "INSERT INTO "+config.tableName+" (data) VALUES ("+values.join('),(')+");";
	if (config.debug) log('Query: %s',insertquery);
	// executing the query:
	db.none(insertquery)
	    .then(data => {
	        // success;
		if (config.debug && data) log('PGP RES: %s',data);
		return data;
	    })
	    .catch(error => {
	        // error;
		if (config.debug) log('PGP ERR: %s',error);
		return error;
	    });
};
