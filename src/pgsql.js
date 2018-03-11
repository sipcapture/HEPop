/*
 * HEPop PGsql connector
 *
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
const pgp = require('pg-promise')({
    /* initialization options */
    capSQL: true // capitalize all generated SQL
});
var config = require('./config').getConfig();
var cs;
var db;

var prepare = function(name){
	var query = "CREATE TABLE IF NOT EXISTS "+name+" (ID serial NOT NULL PRIMARY KEY, data json NOT NULL );"
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
    exports.pgp = db;
    var prep = prepare(config.dbName);
    log('%start:cyan Initializing PGSql driver [%s:blue]', stringify(config.db.pgsql));
    log('%start:cyan Initializing PGSql table [%s:blue]', prep);
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
	prepare(name,db);
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
	  return "'"+stringify(item).replace(/,}$/,"}")+"'";
	});
	var insertquery = "INSERT INTO "+config.dbName+" (data) VALUES ("+values.join('),(')+")";
	if (config.debug) log('Query: %s',insertquery);
	// executing the query:
	db.none(insertquery)
	    .then(data => {
	        // success;
		if (config.debug) log('PGP RES: %s',data);
		return data;
	    })
	    .catch(error => {
	        // error;
		if (config.debug) log('PGP ERR: %s',error);
		return error;
	    });
};
