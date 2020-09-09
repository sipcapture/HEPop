/*
 * HEPop Cloudwatch connector
 *
 * https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/CloudWatchLogs.html
 *
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var config = require('./config').getConfig();
var AWS = require('aws-sdk/');

if(!config.db.cloudwatch) {
    log('%stop:red Missing configuration for Cloudwatch');
    process.exit();
    //return;
}

var settings = {
    "region": config.db.cloudwatch.region || "us-east-1",
    // Log group names must be unique within a region for an AWS account.
    // set them up first, and then add to config
    "log_group": config.db.cloudwatch.log_group || "homer",
    // aws sdk api version
    "api_version": config.db.cloudwatch.api_version || "2014-03-28",
    "invalid_token_retries": config.db.cloudwatch.invalid_token_retries || 3
};

if(!settings.log_group) {
    log('%stop:red Invalid configuration for Cloudwatch [%s:blue]', settings.log_group);
    process.exit();
}

AWS.config.update({region: settings.region});
var cloudwatchlogs = new AWS.CloudWatchLogs({apiVersion: settings.api_version});
var sequenceToken = "";

// A log stream is a sequence of log events that originate from a single source,
// such as an application instance or a resource that is being monitored.
const initalizeLogStream = function(id){
    var params = {
        logGroupName: settings.log_group,
        logStreamName: id
    };
    return new Promise(function(resolve, reject) {
        var createLogStreamPromise = cloudwatchlogs.createLogStream(params).promise();
        createLogStreamPromise.then(function(data) {
            log('%start:green LogStream created: %s', id);
            resolve();
        }).catch(function(err) {
            if (err.code == 'ResourceAlreadyExistsException') {
                if (config.debug) log('%data:green LogStream already exists: %s', id);
                resolve();
            } else {
                reject(err)
            }
        });
    });

};

const putLogs = function(events, id, token, retries) {
    var params = {
        logEvents: events,
        logGroupName: settings.log_group,
        logStreamName: id
    };
    // when a new logstream is created, there is no token,
    // and passing an empty token is an invalid api request
    if (token != "" && token != "null") {
        params.sequenceToken = token
    }
    if (retries > settings.invalid_token_retries) {
        return new Promise(function(resolve, reject) {reject("too many retries")});
    }
    return new Promise(function(resolve, reject) {
        var putLogsPromise = cloudwatchlogs.putLogEvents(params).promise();
        putLogsPromise.then(function(data) {
            resolve(data);
        }).catch(function(err) {
            if (err.code == 'InvalidSequenceTokenException') {
                if (config.debug) log('%error:red Invalid sequence token: %s', err.message);
                // get next sequence token and retry
                tok = err.message.substr(err.message.indexOf('is:') + 4);
                if (config.debug) log('%start:red Retry with sequence token: %s', tok);
                putLogs(events, id, tok, retries+1).then(function(res) {
                    resolve(res);
                })
                .catch(function(err) {
                    log('%error:red Retry %d to Put LogEvents failed: %s', retries, err);
                    reject(err);
                });
            } else {
                reject(err);
            }
        });
    });
}

// original data in structure
//  [{protocol_header,data_header,create_date,raw,sid}]
// use the create_date value to set structure for Cloudwatch
//  [{message, timestamp}]
const setTimestamps = (items) => items.map(
	(item) => {
    	ts = (new Date(item.create_date)).getTime()
        return {"timestamp": ts, "message": JSON.stringify(item)}
    }
);

// Generating a multi-log put into aws cloudwatch
exports.insert = function(bulk,id){
    // if (config.debug) log('LOG STREAM: %s',id);
    // if (config.debug) log('BULK DATA: %s',JSON.stringify(bulk));
    initalizeLogStream(id).then(function() {
        var logEvents = setTimestamps(bulk)
        putLogs(logEvents, id, sequenceToken, 0).then(function(res) {
            // SUCCESS
            if (config.debug) log('%start:green Logs saved');
            sequenceToken = res.nextSequenceToken
        })
        .catch(function(err) {
            log('%error:red Put LogEvents failed: %s', err);
        });
    })
    .catch(function(err) {
        log('%error:red LogStream creation failed: %s', err);
    });
}
