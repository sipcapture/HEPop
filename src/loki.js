/*
 * HEPop Loki connector
 *
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var config = require('./config').getConfig();
var cs;
var db;

const axios = require('axios');


if(!config.db.pgsql) {
    log('%stop:red Missing configuration for PGsql [%s:blue]');
    process.exit();
    //return;
  }


var rawSize = config.db.rawSize || 8000;

// Generating a multi-row insert to /api/prom/push
exports.insert = function(bulk,id){
	if (id && !tables[id]) {
	    log('%stop:red Missing ID for Loki [%s:blue]');
		return false;

	}

	if (config.debug) log('GOT LOKI BULK ID:%s: %s',id, JSON.stringify(bulk));

	// FORM Loki API Post body
	var line = {"streams": [{"labels": "", "entries": [] }]};
	var results = 0;
	var labels = "";
        line.streams[0].labels="__name__=\""+id+"\""
             bulk.forEach(function(row){
		// results++;
		console.log(row);
                // line.streams[0].entries.push({ "ts": row['@timestamp']||new Date().toISOString(), "line": row.message  });
             });

	// POST Bulk to Loki
	if (results>0){
		axios.post(config.db.loki.url, line)
		  .then(function (response) {
		    console.log(response);
		  })
		  .catch(function (error) {
		    console.log(error);
		  });
	}

};
