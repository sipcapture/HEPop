var events = require('events');

function Bucket(opts,uuid) {
	this.bulk = [];
	this.timeout = opts.timeout || 10000;
	this.maxSize = opts.maxSize || 1000;
	this.useInterval = opts.useInterval || opts.use_interval || false;
	this.emitter = new events.EventEmitter();
	this.timer = null;
	this.uuid = uuid;
	if (this.useInterval) {
		this.set_timer();
	}
}

Bucket.prototype.set_timer = function() {
	if (this.useInterval == false) {
		return;
	}

	if (this.timer) {
		clearTimeout(this.timer);
	}

	var self = this;
	this.timer = setTimeout(function() {
		// console.log('timeout')
		self.emit();
		self.set_timer();
	}, this.timeout);
}

Bucket.prototype.emit = function() {
	if (this.bulk.length == 0) {
		return;
	}
	this.emitter.emit('data', this.bulk.slice(), this.uuid);
	this.bulk = [];
}

Bucket.prototype.check = function() {
	if (this.bulk.length >= this.maxSize) {
		this.emit();
		this.set_timer();
		return;
	}

	if (!this.useInterval)
		this.set_timer();
}

function BucketExternal(bucket) {
	this.bucket = bucket;
}


BucketExternal.prototype.on = function(event, cb) {
	this.bucket.emitter.on(event, cb);
	return this;
}

BucketExternal.prototype.push = function(data, cb) {
	if (data == null) {
		this.emitter.emit(new Error('data is empty'));
	}

	this.bucket.bulk.push(data);
	this.bucket.check();
}

BucketExternal.prototype.set_id = function(id, cb) {
	console.log('SET Bucket ID:',id);
	this.bucket.uuid = id;
}

BucketExternal.prototype.close = function(cb) {
	clearTimeout(this.bucket.timer);
	if (cb && typeof cb == 'function') {
		cb(this.bucket.bulk.slice());
		this.bucket.bulk = [];
	}
}

function create(opts,uuid) {
	return new BucketExternal(new Bucket(opts,uuid));
}

module.exports = {
	create: create
}
