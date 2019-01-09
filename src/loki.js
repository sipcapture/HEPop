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


if(!config.db.loki) {
    log('%stop:red Missing configuration for Loki [%s:blue]');
    process.exit();
    //return;
  }

var lokiApi = axios.create({
  baseURL: config.db.loki.url,
  timeout: 1000,
});
lokiApi.defaults.headers.post['Content-Type'] = 'application/json';

/* helpers */

const groupBy = (items, key) => items.reduce(
  (result, item) => ({
    ...result,
    [item.raw[key]]: [
      ...(result[item.raw[key]] || []),
      item,
    ],
  }), 
  {},
);

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
	var count = 0;
	var groups = 0;
	var labels = "";
	var dataset = groupBy(bulk,'type');

	for (var xid in dataset){
	     line.streams[count].labels="{type=\"json\", id=\""+xid+"\"}"
	     dataset[key].forEach(function(row){
		if (config.debug) console.log('PROCESSING LOKI BULK',xid, row);
                line.streams[count].entries.push({ "ts": row['create_date']||new Date().toISOString(), "line": JSON.stringify(row.raw)  });
             });
	     count++;
	}
	line = JSON.stringify(line);
	// POST Bulk to Loki
	if (line){
		lokiApi.post(config.db.loki.url, line)
		  .then(function (response) {
		    if (config.debug) console.log('LOKI RESP',response);
		  })
		  .catch(function (error) {
		    console.log('LOKI ERR',error);
		  });
	}

};
