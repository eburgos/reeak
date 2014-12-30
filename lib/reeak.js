/**
 * Created by eburgos on 12/29/14.
 */

'use strict';

var Buckets = require('./Buckets'),
	Bucket = require('./Bucket');
var Q = require('kew');
var request = require('request');

function Db (args) {
	this.servers = args.pool.servers;
	this.args = args;
	var ns = (args || {}).namespace || '';
	if (ns) {
		ns = '/' + ns;
	}
	this.urlPrefix = this.servers[0] + ns;
	this.buckets = new Buckets (this);
}
Db.prototype.request = function (args) {
	args.url = (this.args.https?'https://':'http://') + this.urlPrefix + args.url;
	var defer = Q.defer();
	request(args, function (error, response, body) {
		if (error) {
			defer.reject(error);
		}
		else {
			defer.resolve({ response: response, body: body });
		}
	});
	return defer.promise;
};
Db.prototype.bucket = function (name) {
	return new Bucket(this, name);
};
module.exports.Db = Db;