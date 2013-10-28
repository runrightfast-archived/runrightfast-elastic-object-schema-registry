/**
 * Copyright [2013] [runrightfast.co]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

'use strict';
var expect = require('chai').expect;
var ObjectSchemaRegistryDatabase = require('..').ObjectSchemaRegistryDatabase;
var ObjectSchema = require('runrightfast-validator').validatorDomain.ObjectSchema;
var when = require('when');
var uuid = require('runrightfast-commons').uuid;
var lodash = require('lodash');

var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId;

var ElasticSearchClient = require('runrightfast-elasticsearch').ElasticSearchClient;
var ejs = new ElasticSearchClient({
	host : 'localhost',
	port : 9200
}).ejs;

describe('database', function() {
	var database = new ObjectSchemaRegistryDatabase({
		ejs : ejs,
		index : 'objectschema',
		type : 'objectschema',
		entityConstructor : ObjectSchema,
		logLevel : 'DEBUG'
	});

	var idsToDelete = [];

	afterEach(function(done) {
		database.deleteObjectSchemas(idsToDelete).then(function(result) {
			idsToDelete = [];
			done();
		}, function(error) {
			console.error(JSON.stringify(error, undefined, 2));
			done(error.error);
		});
	});

	it('can create a new ObjectSchema in the database', function(done) {
		var schema = new ObjectSchema({
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		});
		database.createObjectSchema(schema).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			idsToDelete.push(result._id);
			done();
		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('can get an ObjectSchema by id', function(done) {
		var schema = new ObjectSchema({
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		});
		database.createObjectSchema(schema).then(function(result) {
			console.log('create response: ' + JSON.stringify(result, undefined, 2));
			idsToDelete.push(result._id);

			database.getObjectSchema(result._id).then(function(result) {
				console.log('retrieved : ' + JSON.stringify(result, undefined, 2));
				var objectSchema = new ObjectSchema(result._source);
				done();
			}, done);

		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

});