/**
 * Created by eburgos on 12/29/14.
 */


'use strict';

var request = require('request');

/*
Handles all operations regarding a bucket.
@constructor
@param {object} reeak - Reeak instance
@param {string} bucketName - current bucket's name
 */
function Bucket (reeak, bucketName) {
	this.reeak = reeak;
	this.bucketName = bucketName;
}
/*
 Returns all keys from a specific bucket. NOTE: This operation has high performance impact.
 if chunked is false it returns Promise<string list>, otherwise if returns nothing and keeps calling the callbacks.
 @param {boolean} chunked - true if you need the response to be streamed. Must provide onData callback
 @param {Function} onData - callback to be called whenever data comes out of the request. onData has the form function (dataArray) {}
 @param {Function} onEnd - callback to be called whenever the chunked request is finished. onEnd has the form function (error) {}
 */
Bucket.prototype.allKeys = function (chunked, onData, onEnd) {
	if (chunked) {
		var r = this.reeak
			.request({ url: '/buckets/' + this.bucketName + '/keys?keys=stream', method: 'GET' }, function (error) {
				onEnd(error);
			});
		r.on('data', function (data) {
			var keys = JSON.parse(data).keys;
			onData(keys);
		});
	}
	else {
		return this.reeak
			.request({ url: '/buckets/' + this.bucketName + '/keys?keys=true', method: 'GET' })
			.then(function (data) {
				var response = data.response;
				if ((response.statusCode === 200) && (response.contentType === 'application/json')) {
					return JSON.parse(data.body).keys;
				}
				else {
					throw new Error('reeak: HTTP List Keys - Invalid response: ' + response.statusCode);
				}
			});
	}
};
var getObject = function (self, params, qsargs, mergeStrategy) {
	if (qsargs) {
		params.qs = qsargs;
	}
	if (mergeStrategy) {
		params.headers.Accept = 'multipart/mixed;q=0.9,application/json';
	}
	return self.reeak
		.request(params)
		.then(function (data) {
			var response = data.response;
			if (response.statusCode === 200) {
				return { response: response, data: data.body };
			}
			else if (response.statusCode === 300 /* multiple choices */) {
				throw new Error('Not implemented multiple choices');
			}
			else if (response.statusCode === 404) {
				return { response: response, data: null };
			}
			else {
				throw new Error('reeak: HTTP Get - Invalid response: ' + response.statusCode);
			}
		});
};
/*
 Returns a value by key
 */
Bucket.prototype.get = function (key, args, mergeStrategy) {
	var params = { url: '/buckets/' + this.bucketName + '/keys/' + key, method: 'GET', headers: {} };
	return getObject (this, params, args, mergeStrategy);
};
/*
 Returns the keys corresponding to a single-valued index lookup
 */
Bucket.prototype.keysFromIndex = function (name, value, args, mergeStrategy) {
	var params = { url: '/buckets/' + this.bucketName + '/index/' + name + '_' + indexSuffix(value) + '/' + value, method: 'GET', headers: {} };
	return getObject (this, params, args, mergeStrategy)
		.then (function (resp) {
			resp.data = JSON.parse(resp.data).keys;
			return resp;
		});
};
/*
 Returns the objects corresponding to a single-valued index lookup. This operations invokes a mapreduce process by index lookup.
 WARNING: This assumes that all stored objects are in JSON format
 */
Bucket.prototype.objectsFromIndex = function (name, value, args, mergeStrategy) {
	var params = {
		url: '/mapred', //'/buckets/' + this.bucketName + '/index/' + name + '_' + indexSuffix(value) + '/' + value,
		method: 'POST',
		headers: {
			'Content-Type': 'application/json'
		},
		body: '{"inputs":{ "bucket": "' + this.bucketName + '", "index": "' + name + '_' + indexSuffix(value) +'", "key": "' + value + '" },"query":[{"map":{"language":"javascript","source":"function(value) { return [value]; }","keep":true}}]}'
	};
	return getObject (this, params, args, mergeStrategy)
		.then(function (resp) {
			var vals = JSON.parse(resp.data).map(function (v) {
				if (v.values.length > 1) {
					return mergeStrategy(v);
				}
				else {
					return JSON.parse(v.values[0].data);
				}
			});
			resp.data = vals;
			return resp;
		});
};
function indexSuffix (val) {
	if (Number.isInteger(val)) {
		return 'int';
	}
	return 'bin';
}
/*
 Saves value to a bucket.
 */
Bucket.prototype.save = function (key, obj, args, qsargs) {
	var params = {
			method: 'PUT',
			headers: {}
		},
		p,
		cur;
	args = args || {};
	if (!key) {
		params.url = '/buckets/' + this.bucketName + '/keys';
		params.method = 'POST';
	}
	else {
		params.url = '/buckets/' + this.bucketName + '/keys/' + key;
	}
	if (qsargs) {
		params.qs = qsargs;
	}
	if (typeof(obj) === 'object') {
		params.headers['Content-Type'] = 'application/json';
		params.body = JSON.stringify(obj);
	}
	else {
		params.body = obj;
	}
	if (typeof(args.headers) === 'object') {
		for (p in args.headers) {
			if (args.headers.hasOwnProperty(p)) {
				params.headers[p] = args.headers[p];
			}
		}
	}
	if (typeof(args.vclock) === 'string') {
		params.headers['X-Riak-Vclock'] = args.vclock;
	}
	if (typeof(args.meta) === 'object') {
		for (p in args.meta) {
			if (args.meta.hasOwnProperty(p)) {
				params.headers['X-Riak-Meta-' + p] = args.meta[p];
			}
		}
	}
	if (typeof(args.index) === 'object') {
		for (p in args.index) {
			if (args.index.hasOwnProperty(p)) {
				cur = args.index[p];
				params.headers['X-Riak-Index-' + p + '_' + indexSuffix(cur)] = cur;
			}
		}
	}
	if (typeof(args.links) === 'object') {
		var links = Object.keys(args.links).map(function (item) { return '</riak/' + args.links[item] + '>; riaktag="' + item + '"' ; });
		if (links.length) {
			params.headers['Link'] = links.join(', ');
		}
	}
	return this.reeak
		.request(params)
		.then(function (data) {
			var response = data.response;
			if ((response.statusCode === 200) || (response.statusCode === 204)) {
				return data.body;
			}
			else if (response.statusCode === 201 /* created */) {
				return response.headers['Location'];
			}
			else if (response.statusCode === 300 /* multiple choices */) {
				throw new Error('Not implemented multiple choices');
			}
			else {
				throw new Error('reeak: HTTP Save - Invalid response: ' + response.statusCode);
			}
		});
};
/*
 Deletes a value from a bucket.
 */
Bucket.prototype.delete = function (key, qsargs) {
	var params = {
			method: 'DELETE',
			headers: {}
		};
	params.url = '/buckets/' + this.bucketName + '/keys/' + key;

	if (qsargs) {
		params.qs = qsargs;
	}

	return this.reeak
		.request(params)
		.then(function (data) {
			var response = data.response;
			if (response.statusCode === 204) {
				return true;
			}
			else if (response.statusCode === 404 /* not found */) {
				return false;
			}
			else {
				throw new Error('reeak: HTTP Save - Invalid response: ' + response.statusCode);
			}
		});
};


module.exports = Bucket;