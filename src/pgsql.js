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


var createTable = function(tableName){
    var doTable = "CREATE TABLE IF NOT EXISTS "+tableName+" "
		+ "(id BIGSERIAL NOT NULL, gid smallint DEFAULT '0', sid varchar(256), create_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
 		+ "protocol_header json NOT NULL, data_header jsonb NOT NULL, raw varchar(2000) NOT NULL, PRIMARY KEY (id,create_date) );"
    prepare(doTable);
}

try {
    db = pgp(config.db.pgsql);
    db.connect();
    exports.pgp = db;
    log('%start:cyan Initializing PGSql driver [%s:blue]', stringify(config.db.pgsql));
    //var doDb = "CREATE DATABASE "+config.dbName;
    //prepare(doDb);
} catch(err){
    log('%stop:red Failed Initializing PGsql driver [%s:blue]',err);
    process.exit();
    // return;
}


if (!config.db.pgsql.schema){
  var tableSchema = config.db.pgsql.schema || ['sid', 'protocol_header','data_header','raw'];
  var tableName = {table: config.tableName || 'homer_data' };
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


var tables = [];

// Generating a multi-row insert query:
// insert(values, cs);
// => INSERT INTO "hepic"("col_a","col_b") VALUES('a1','b1'),('a2','b2')
exports.insert = function(bulk,id){
	if (id && !tables[id]) {
	    var tableSchema = config.db.pgsql.schema || ['sid', 'protocol_header','data_header','raw'];
	    var tableName = {table: id };
	    cs = new pgp.helpers.ColumnSet(tableSchema, tableName);
	    tables[id] = cs;
	    createTable(id);
	} else { cs = tables[id] }

	if (!cs) {
	    log('%stop:red Missing Schema/Templace for PGsql [%s:blue]');
	    return false;
	}

	if (config.debug) log('GOT BULK ID:%s: %s',id, JSON.stringify(bulk));

	const insertquery = pgp.helpers.insert(bulk, cs);
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
		if (error.toString().indexOf('does not exist') !== -1) {
			createTable(id);
		}
		return error;
	    });
};
