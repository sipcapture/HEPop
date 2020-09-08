/*
 * HEPop Cloudwatch connector
 *
 */

const log = require('./logger');
const stringify = require('safe-stable-stringify');
var config = require('./config').getConfig();
var AWS = require('aws-sdk/');

// TODO: get region from config
AWS.config.update({region: 'us-east-1'});
// TODO: get apiversion from config
var cloudwatchlogs = new AWS.CloudWatchLogs({apiVersion: '2014-03-28'});


if(!config.db.cloudwatch) {
    log('%stop:red Missing configuration for Cloudwatch [%s:blue]');
    process.exit();
    //return;
}
// TODO: Log group names must be unique within a region for an AWS account.
// set them up first, and then add to config



// A log stream is a sequence of log events that originate from a single source,
// such as an application instance or a resource that is being monitored.
const initalizeLogStream = function(id, group){
    var params = {
        logGroupName: group,
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

const putLogs = function(events, id, group) {
    var params = {
        logEvents: events,
        logGroupName: group,
        logStreamName: id
    };
    cloudwatchlogs.putLogEvents(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else     console.log(data);           // successful response
    });
}

// original data in structure
//  [{protocol_header,data_header,create_date,raw,sid}]
// use the create_date value to set structure for Cloudwatch
//  [{message, timestamp}]
const setTimestamps = (items) => items.map(
	(item) => {
    	ts = (new Date(item.create_date)).getTime()
        return {"time": ts, "message": item}
    }
);

// Generating a multi-log put into aws cloudwatch
exports.insert = function(bulk,id){
    group = 'QXIP/heplify';
    // if (config.debug) log('LOG STREAM: %s',id);
    // if (config.debug) log('BULK DATA: %s',JSON.stringify(bulk));
    initalizeLogStream(id).then(function() {
        var logEvents = setTimestamps(bulk)
        putLogs(logEvents, id, group).then(function(res) {
            // SUCCESS
            console.log("success", res)
        })
        .catch(function(err) {
            log('%error:red Put LogEvents failed: %s', err);
        });
    })
    .catch(function(err) {
        log('%error:red LogStream creation failed: %s', err);
    });
}
