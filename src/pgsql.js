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

if(!config.db.pgsql) {
    log('%stop:red Missing configuration for PGsql [%s:blue]');
    process.exit();
    //return;
  }

try {
    const db = pgp(config.db.pgsql);
} catch(err){
    log('%stop:red Failed Initializing PGsql [%s:blue]');
    process.exit();
    // return;
}


if (config.db.pgsql.schema){
  var tableSchema = config.db.pgsql.schema || ['col_a', 'col_b'];
  var tableName = {table: config.tableName || 'hepic' };
  cs = new pgp.helpers.ColumnSet(tableSchema, tableName);
}

// PGSql connector;
exports.pgp = db;

// Column template to be generated and then shared/cached/reused for high performance:
exports.setTemplate = function(schema,name){
	if (!schema||!name) return false;
	cs = new pgp.helpers.ColumnSet(schema, name);
	return true;
};


// Generating a multi-row insert query:
// insert(values, cs);
// => INSERT INTO "hepic"("col_a","col_b") VALUES('a1','b1'),('a2','b2')
exports.insert = function(bulk){
	if (!cs) {
	    log('%stop:red Missing Schema/Templace for PGsql [%s:blue]');
	    return false;
	}
	var query = pgp.helpers.insert(bulk,cs); // params: bulk_array, template
	// executing the query:
	db.none(query)
	    .then(data => {
	        // success;
		return data;
	    })
	    .catch(error => {
	        // error;
		return error;
	    });
};
