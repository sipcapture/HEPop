var log = require('./logger');
const stringify = require('safe-stable-stringify');

var sharedConfig = {};

exports.setConfig = function(config){
	sharedConfig = config;
	log('%data:cyan CONFIG set [%s:blue]', stringify(sharedConfig) );
};
exports.getConfig = function(){
	return sharedConfig;
};
