/*!
 * es-broker.js
 * ISC Licensed
 * Based on https://github.com/ejjoo/node-elasticsearch-broker
 */

const log = require('./logger');

var broker = function (opts) 
{
	var async = require('async'),
		elasticsearch = require('elasticsearch'),
		util = require('util'),
		bucket_emitter = require('./bulk-emitter');

	var _verbose = opts.verbose || false,
		_read_count = 0,
		_total_read_count = 0,
		_readstream = opts.stream,
		_parse_job = opts.parser || function(data, callback) { callback(null, data);},
		_max_bulk_qtty = opts.max_bulk_qtty || 1000,
		_max_request_num = opts.max_request_num || 20,
		_target = opts.target || 'http://localhost:9200',
		_index = opts.index,
		_verbose_data = opts.verbose_data || false;

	var esclient = elasticsearch.Client({
		host: _target
	});
	
	if (esclient == null) {
		throw new Error("failed to create elasticSearch client");
	}

	if (_index == null) {
		throw new Error('elasticsearch index is not passed');
	}

	if (_parse_job == null || typeof(_parse_job) != 'function') {
		throw new Error("no parse job");
	}

	var _es_bulk_queue = async.queue(function(data, callback) {
		if (data == null 
			|| typeof(data) != 'object' 
			|| data.body == null) {
			callback(new Error('request body is invalid'), null);
		}

		esclient.bulk(data, function(err, res) {
			callback(err, res);
		});
	}, _max_request_num);

	var _enqueue_request = function(data, cb) {
		cb = cb || function() {}
		var print_q_status = function(name, q) {
      log('%start:green Running [%s:blue] wait [%s:red]', q.running().toString(), q.length().toString() );
		}

		var count = _total_read_count;
		_es_bulk_queue.push({body: data}, function(err, res) {
			if (err) {
        log('%stop:red Error [%s:red]', err );
				return;
			}

			if (res.errors) {
				cb(res.errors, null);
				return;
			}

			if (_verbose) {
				print_q_status('Parseing Queue', _parse_queue);
				print_q_status('Request Queue', _es_bulk_queue);
        log('%check:green Total Sent [%s:yellow]', count );
			} 

			cb(null, count);
		});	
	}

	var _parse_queue = async.queue(_parse_job);
	var index = {
		"index": {
			"_index": _index,
			"_type": 'error'
		}
	};

	var bucket = bucket_emitter.create({
		timeout: 10000,
		use_interval: false,
		maxSize: _max_bulk_qtty * 2
	});

	bucket.on('data', function(data) {
		_total_read_count += _read_count;
		_enqueue_request(data, null);
		_read_count = 0;
	})

	var wait_until_end = function(cb) {
		if (_parse_queue.length() + _parse_queue.running() != 0) {
			setTimeout(function() {
				wait_until_end(cb)
			}, 0);
			return;
		}

		_total_read_count += _read_count;
		bucket.close(function(data) {
			_enqueue_request(data, function() {
				if (cb && typeof cb == 'function') {
					cb();
				}
			});
		})
	}

	return {
		push: function(line) {
			_read_count++;
			_parse_queue.push(line, function(err, parsed_data) {
				if (err) {
          log('%stop:red Parse Error [%s:red] %s', err, line );
					return;
				}

				if (util.isArray(parsed_data)) {
					var len = parsed_data.length;
					for (var i=0; i<len; i++) {
						bucket.push(index);
						bucket.push(parsed_data[i]);
					}

				} else {
					bucket.push(index);
					bucket.push(parsed_data);						
				}
			});
		},
		close: function(cb) {
			wait_until_end(function() {
				if (cb && typeof cb == 'function')
					cb(_total_read_count);
			});
		}
	}
}

if (typeof module === 'object' && module.exports) {
	module.exports = {
		create: broker
	}	
}
else {
  log('%stop:red Unsupported Platform' );
}
