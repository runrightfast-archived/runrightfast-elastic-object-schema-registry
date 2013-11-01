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

	before(function(done){
		database.database.deleteIndex()
		.then(function(result){
			console.log('DELETED INDEX: ' + JSON.stringify(result,undefined,2));
			database.database.createIndex({})				
				.then(function(){
					console.log('CREATED INDEX: ' + JSON.stringify(result,undefined,2));
					database.setMapping().						
						then(function(result){
							console.log('SET MAPPING: ' + JSON.stringify(result,undefined,2));
							done();
						},done);
				},done);
		},done);
	});

	afterEach(function(done) {
		database.deleteObjectSchemas(idsToDelete).then(function() {
			idsToDelete = [];
			database.database.refreshIndex().then(function(){
				done();
			});			
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
		idsToDelete.push(schema.id);
		database.createObjectSchema(schema).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			done();
		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('creating an ObjectSchema with the same namespace and version will fail', function(done) {
		var schema = new ObjectSchema({
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		});
		idsToDelete.push(schema.id);
		database.createObjectSchema(schema).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			database.createObjectSchema(schema).then(function(result) {
				console.log(JSON.stringify(result, undefined, 2));
				done(new Error('expected create to fail'));
			}, function(err) {
				console.error('create failed : ' + err);
				done();
			});
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
		idsToDelete.push(schema.id);

		database.createObjectSchema(schema).then(function(result) {
			console.log('create response: ' + JSON.stringify(result, undefined, 2));
			database.getObjectSchema(result._id).then(function(result) {
				console.log('retrieved : ' + JSON.stringify(result, undefined, 2));
				//validates that the returned object is a valid ObjectSchema
				var objectSchema = new ObjectSchema(result._source);
				console.log('objectSchema : ' + JSON.stringify(objectSchema, undefined, 2));
				done();
			}, done);

		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('can find an ObjectSchema by its namespace and version',function(done){
		var schema = new ObjectSchema({
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		});
		idsToDelete.push(schema.id);

		database.createObjectSchema(schema).then(function(result) {
			console.log('create response: ' + JSON.stringify(result, undefined, 2));

			database.findByNamespaceVersion(schema.namespace,schema.version).then(function(result) {
				console.log('findByNamespaceVersion result : ' + JSON.stringify(result, undefined, 2));
				try{
					expect(result.hits.total).to.equal(1);
					//validates that the returned object is a valid ObjectSchema
					var objectSchema = new ObjectSchema(result.hits.hits[0]._source);
					console.log('objectSchema : ' + JSON.stringify(objectSchema, undefined, 2));
					done();
				}catch(err){
					done(err);
				}
			}, done);

		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('#findByNamespaceVersion - namespace and version are required',function(done){
		var promises = [];
		promises.push (when(database.findByNamespaceVersion(),
			function(result) {
				throw new Error('validation should have failed');
			}, 
			function(err){
				console.log(err);
				return;				
			}));
		promises.push (when(database.findByNamespaceVersion(null,'1.0.0'),
			function(result) {
				throw new Error('validation should have failed');
			}, 
			function(err){
				console.log(err);
				return;				
			}));

		promises.push (when(database.findByNamespaceVersion('ns://user'),
			function(result) {
				throw new Error('validation should have failed');
			}, 
			function(err){
				console.log(err);
				return;				
			}));

		when(when.all(promises),function(){done();},done);
	});

	it('can return the listing of namespaces along with the number of versions per namespace',function(done){
		var schemas = [
			new ObjectSchema({
				namespace : 'ns://runrightfast.co/couchbase',
				version : '1.0.0',
				description : 'Couchbase config schema'
			}),
			new ObjectSchema({
				namespace : 'ns://runrightfast.co/couchbase',
				version : '1.0.1',
				description : 'Couchbase config schema'
			}),
			new ObjectSchema({
				namespace : 'ns://runrightfast.co/couchbase/1',
				version : '2.0.0',
				description : 'Couchbase config schema'
			}),
		];
		schemas.forEach(function(schema){
			idsToDelete.push(schema.id);	
		});	

		var promises = schemas.map(function(schema){
			return database.createObjectSchema(schema);
		});

		when.all(promises).then(function(result) {
			console.log('create response: ' + JSON.stringify(result, undefined, 2));

			database.database.refreshIndex().then(function(){
				database.getObjectSchemaNamespaces().then(function(result){
					console.log('getObjectSchemaNamespaces() : ' + JSON.stringify(result,undefined,2));
					try{
						expect(result['ns://runrightfast.co/couchbase']).to.equal(2);
						expect(result['ns://runrightfast.co/couchbase/1']).to.equal(1);
						done();
					}catch(err){
						done(err);
					}
				});
			},done);

		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('#setObjectSchema', function(done) {
		var schema = new ObjectSchema({
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		});
		idsToDelete.push(schema.id);
		database.createObjectSchema(schema).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			try{
				var type = {
					description : 'person name',
					properties :{
						fname:{
							type: 'String',
							constraints:[
								{method: 'required',args:[]}
							]
						}
					}
				};
				schema.addType('PersonName',type);
				when(database.setObjectSchema(schema,result._version,'azappala'),
					function(result){
						console.log('setObjectSchema() result: ' + JSON.stringify(result,undefined,2));
						done();
					},
					function(err){
						console.error('setObjectSchema() failed : ' + err);
						done(err);
					});
			}catch(err){
				done(err);
			}
		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('#getObjectSchemas',function(done){
		var objectSchemas = [];
		var i;
		for(i=0;i<10;i++){
			objectSchemas.push(new ObjectSchema({
				namespace : 'ns://runrightfast.co/runrightfast-api-gateway',
				version : '1.0.' + i,
				description : 'runrightfast-api-gateway config'
			}));
		}

		var promises = lodash.foldl(objectSchemas,function(promises,objectSchema){
			idsToDelete.push(objectSchema.id);
			promises.push(database.createObjectSchema(objectSchema));
			return promises;
		},[]);

		when(when.all(promises),
			function(result){
				console.log(JSON.stringify(result,undefined,2));
				var ids = objectSchemas.map(function(objectSchema){
					return objectSchema.id;
				});
				when(database.getObjectSchemas(ids),
					function(result){
						try{
							console.log(JSON.stringify(result,undefined,2));
							expect(result.docs.length).to.equal(10);
							done();
						}catch(err){
							done(err);
						}
					}
				)
				
			},
			done
		)
	});

	it('#deleteObjectSchema',function(done){
		var schema = new ObjectSchema({
			namespace : 'ns://runrightfast.co/couchbase',
			version : '1.0.0',
			description : 'Couchbase config schema'
		});
		idsToDelete.push(schema.id);
		database.createObjectSchema(schema).then(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			when(database.deleteObjectSchema(schema.id),
				function(result){
					when(database.getObjectSchema(schema.id),
						function(result){
							console.log(JSON.stringify(result, undefined, 2));
							done(new Error('expected error because entity should not exist'));
						},
						function(err){
							console.log(err);
							done();
						}
					);
				},
				done
			);
		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('#getObjectSchemaVersions',function(done){
		var schemas = [
			new ObjectSchema({
				namespace : 'ns://runrightfast.co/couchbase',
				version : '1.0.0',
				description : 'Couchbase config schema'
			}),
			new ObjectSchema({
				namespace : 'ns://runrightfast.co/couchbase',
				version : '1.0.1',
				description : 'Couchbase config schema'
			}),
			new ObjectSchema({
				namespace : 'ns://runrightfast.co/couchbase/1',
				version : '2.0.0',
				description : 'Couchbase config schema'
			}),
		];
		schemas.forEach(function(schema){
			idsToDelete.push(schema.id);	
		});	

		var promises = schemas.map(function(schema){
			return database.createObjectSchema(schema);
		});

		when.all(promises).then(function(result) {
			console.log('create response: ' + JSON.stringify(result, undefined, 2));

			database.database.refreshIndex().then(function(){
				database.getObjectSchemaVersions('ns://runrightfast.co/couchbase').then(function(result){
					console.log('getObjectSchemaVersions() : ' + JSON.stringify(result,undefined,2));
					try{
						expect(lodash.isArray(result)).to.equal(true);
						expect(result.length).to.equal(2);
						done();
					}catch(err){
						done(err);
					}
				},done);
			},done);

		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});

	it('#getObjectSchemaVersions - namespace is required',function(done){
		database.getObjectSchemaVersions().then(function(result){
			console.log('getObjectSchemaVersions() : ' + JSON.stringify(result,undefined,2));
			done(new Error('expected Error'));
		},function(err){
			console.log(err);
			done();
		});
	});

	it('#getObjectSchemaVersions - more than 10 versions triggers paging behind the scenes',function(done){
		var objectSchemas = [];
		var i;
		for(i=0;i<15;i++){
			objectSchemas.push(new ObjectSchema({
				namespace : 'ns://runrightfast.co/runrightfast-api-gateway',
				version : '1.0.' + i,
				description : 'runrightfast-api-gateway config'
			}));
		}

		var promises = objectSchemas.map(function(schema){
			idsToDelete.push(schema.id);
			return database.createObjectSchema(schema);
		});

		when.all(promises).then(function(result) {
			console.log('create response: ' + JSON.stringify(result, undefined, 2));

			database.database.refreshIndex().then(function(){
				database.getObjectSchemaVersions('ns://runrightfast.co/runrightfast-api-gateway').then(function(result){
					console.log('getObjectSchemaVersions() : ' + JSON.stringify(result,undefined,2));
					try{
						expect(lodash.isArray(result)).to.equal(true);
						expect(result.length).to.equal(15);
						done();
					}catch(err){
						done(err);
					}
				},done);
			},done);

		}, function(err) {
			console.error('create failed : ' + err);
			done(err);
		});
	});


});