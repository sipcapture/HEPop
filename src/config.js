/*
 * HEPop Shared Configuration manager
 *
 */
const fs = require('fs');
const log = require('./logger');
const stringify = require('safe-stable-stringify');

var sharedConfig = {};
var readConfig = function(file){
	var obj = JSON.parse(fs.readFileSync(file, 'utf8'));
	return obj;
}

exports.setConfig = function(config){
  try {
	sharedConfig = config;
	// log('%data:green CONFIG SET [%s:blue]', stringify(sharedConfig) );
	// If config file is provided, override
	if (sharedConfig.configfile) sharedConfig = readConfig(sharedConfig.configfile);
	log('%data:green CONFIG FILE [%s:blue]', stringify(sharedConfig) );

  } catch(e) {  log('%data:red SET CONFIG ERROR [%s:cyan]', err ); process.exit(1); }
};

exports.getConfig = function(){
  try {
	return sharedConfig;

  } catch(e) {  log('%data:red GET CONFIG ERROR [%s:cyan]', err ); process.exit(1); }

};
