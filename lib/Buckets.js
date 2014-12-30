/**
 * Created by eburgos on 12/29/14.
 */


'use strict';

var request = require('request');

function Buckets (reeak) {
	this.reeak = reeak;
}

Buckets.prototype.list = function () {
	return this.reeak
		.request({ url: '/buckets?buckets=true', method: 'GET' })
		.then(function (data) {
			var response = data.response;
			if ((response.statusCode === 200) && (response.headers['content-type'] === 'application/json')) {
				return JSON.parse(data.body).buckets;
			}
			else {
				throw new Error('reeak: HTTP List Buckets - Invalid response: ' + response.statusCode);
			}
		});
};


module.exports = Buckets;