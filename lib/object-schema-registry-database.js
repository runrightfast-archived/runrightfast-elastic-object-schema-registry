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

/**
 * <code>
 * options = { 
 * 	 ejs: ejs,								// REQUIRED - elastic.js - ejs
 *   logLevel : 'WARN',						// OPTIONAL - Default is 'WARN',
 *   entityConstructor,						// REQUIRED - Entity constructor,
 *   index: 'index_name'					// REQUIRED - Index name - where the entity will be stored,
 *   type: 'document_type'					// REQUIRED - Type within the Index
 * }
 * </code>
 */
(function() {
	'use strict';

	var lodash = require('lodash');
	var EntityDatabase = require('runrightfast-elasticsearch').EntityDatabase;
	var ObjectSchema = require('runrightfast-validator').validatorDomain.ObjectSchema;
	var when = require('when');

	var ObjectSchemaRegistryDatabase = function(options) {
		options.entityConstructor = ObjectSchema;
		this.database = new EntityDatabase(options);
	};

	var handleSearchError = function(error) {
		throw error;
	};

	/**
	 * 
	 * 
	 * @param objectSchema
	 *            REQUIRED
	 * @return Promise that returns the elasticsearch response
	 */
	ObjectSchemaRegistryDatabase.prototype.createObjectSchema = function(objectSchema) {
		if (objectSchema) {
			var self = this;
			return when(this.findByNamespaceVersion(objectSchema.namespace,objectSchema.version),
						function(result){
							if(result.hits.total > 0){
								throw new Error('An ObjectSchema with the same namespace and version already exists: ' + JSON.stringify(result.hits.hits[0]._source));
							}
							return self.database.createEntity(objectSchema, true);
						},
						handleSearchError
					);			
		}
	};

	/**
	 * 
	 * @param id
	 *            REQUIRED
	 * @return Promise that returns the elasticsearch response
	 * 
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchema = function(id) {
		return this.database.getEntity(id);
	};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
	 *            REQUIRED
	 * @return Promise that returns the elasticsearch response
	 * 
	 */
	ObjectSchemaRegistryDatabase.prototype.findByNamespaceVersion = function(namespace, version) {		
		var self = this;
		var ejs = this.database.ejs;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(namespace) || !lodash.isString(version) || namespace.trim().length === 0 || version.trim().length === 0) {
				reject(new Error('namespace and version are required and cannot be blank'));
				return;
			}
				
			var request = self.database.request();
			var filter = ejs.BoolFilter();
			filter.must(ejs.TermFilter('namespace',namespace));
			filter.must(ejs.TermFilter('version',version));
			request.filter(filter);
			request.doSearch(resolve,reject);
		});
	};

	/**
	 * The updatedOn will be set to the current time. If the ObjectSchema does
	 * not exist, then it will be created.
	 * 
	 * @param objectSchema
	 * @param version
	 *            OPTIONAL used to ensure that no one else has updated the
	 *            schema since it was retrieved
	 * @param updatedBy
	 *            OPTIONAL
	 * @return Promise returning elasticsearch response
	 */
	ObjectSchemaRegistryDatabase.prototype.setObjectSchema = function(objectSchema, version, updatedBy) {
		var params = {
			entity : objectSchema,
			version : version,
			updatedBy : updatedBy
		};
		return this.database.setEntity(params);
	};

	/**
	 * 
	 * @param ids
	 *            REQUIRED - Array of object schema ids. An ObjectSchema id can
	 *            be created using :
	 * 
	 * <code>
	 * var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId
	 * var id = objectSchemaId(namespace,version);
	 * </code>
	 * @return Promise that returns elasticsearch response
	 * 
	 * ObjectSchemas that were found.
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemas = function(ids) {
		return this.database.getEntities(ids);
	};

	/**
	 * 
	 * 
	 * @param namespace
	 * @param version
	 * @return Promise
	 */
	ObjectSchemaRegistryDatabase.prototype.deleteObjectSchema = function(id) {
		var self = this;
		return when.promise(function(resolve, reject) {
			when(self.database.deleteEntity(id), resolve, reject);
		});

	};

	/**
	 * 
	 * 
	 * @param ids
	 *            REQUIRED - Array of object schema ids. An ObjectSchema id can
	 *            be created using :
	 * 
	 * <code>
	 * var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId
	 * var id = objectSchemaId(namespace,version);
	 * </code>
	 * 
	 * @return Promise where the result is the elasticsearch response
	 */
	ObjectSchemaRegistryDatabase.prototype.deleteObjectSchemas = function(ids) {
		return this.database.deleteEntities(ids);
	};

	/**
	 * 
	 * @param namespace
	 * @return Promise that returns the existing versions for the specified
	 *         ObjectSchema namespace - as an Array of strings
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemaVersions = function(namespace) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(namespace)) {
				reject(new Error('namespace is required'));
				return;
			}

			var handleSearchResults = function(results) {				
				return lodash.foldl(results.hits.hits, function(versions, item) {
					versions.push(item.fields.version);
					return versions;
				}, []);
			};

			when(self.database.findByField({
				field : 'namespace',
				value : namespace,
				returnFields : [ 'version' ]
			}), function(results) {
				var versions = handleSearchResults(results);
				if (results.hits.hits.length < results.hits.total) {					
					var from = results.hits.hits.length;
					var maxPageSize = results.hits.total;
					var pageSize = (results.hits.total < maxPageSize) ? results.hits.total : maxPageSize;
					
					when(self.database.findByField({
							field : 'namespace',
							value : namespace,
							returnFields : [ 'version' ],
							from : from,
							pageSize : pageSize
						}), 
						function(results){
							resolve(versions.concat(handleSearchResults(results)));
						},
						handleSearchError
					);
				} else {
					resolve(versions);
				}

			}, reject);
		});

	};

	/**
	 * 
	 * @param namespace
	 * @return Promise that returns a listing of the existing ObjectSchema
	 *         namespaces along with the number of schema versions as an Object
	 *         hash (namespace -> count of versions)
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchemaNamespaces = function() {
		return when(this.database.findAll({
						pageSize:1,
						facet:{
							name:'namespace',
							field: 'namespace'
						}
					}),
					function(result) {					
						if(result.facets.namespace.total > 0){
							return lodash.foldl(result.facets.namespace.terms,function(namespaces,term){
								namespaces[term.term] = term.count;
								return namespaces;
							},{});
						}
						return {};										
					},
					handleSearchError
				);		
	};

	ObjectSchemaRegistryDatabase.prototype.findObjectSchemasByField = function(params) {
		return this.database.findByField(params);
	};



	ObjectSchemaRegistryDatabase.prototype.setMapping = function(){
		var mapping = {
			    "objectschema": {
			       "_source" : {"compress" : true},
			       "properties": {
			            "_entityType": {
			                "type": "string",
			                "index" : "not_analyzed"
			            },
			            "createdOn": {
			                "type": "date",
			                "format": "dateOptionalTime"
			            },
			            "description": {
			                "type": "string"
			            },
			            "id": {
			                "type": "string",
			                "index" : "not_analyzed"
			            },
			            "namespace": {
			                "type": "string",
			                "index" : "not_analyzed"
			            },
			            "types": {
			                "type": "object"
			            },
			            "updatedOn": {
			                "type": "date",
			                "format": "dateOptionalTime"
			            },
			            "version": {
			                "type": "string",
			                "index" : "not_analyzed"
			            }
			        }     
			}
		};

		return this.database.setMapping(mapping);
	};
	

	module.exports = ObjectSchemaRegistryDatabase;
}());
