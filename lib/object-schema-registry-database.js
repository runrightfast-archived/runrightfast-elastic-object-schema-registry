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
	var objectSchemaId = require('runrightfast-validator').validatorDomain.objectSchemaId;
	var when = require('when');

	var ObjectSchemaRegistryDatabase = function(options) {
		options.entityConstructor = ObjectSchema;
		this.database = new EntityDatabase(options);
	};

	/**
	 * 
	 * 
	 * @param objectSchema
	 *            REQUIRED
	 * @return Promise that returns the elasticsearch response
	 */
	ObjectSchemaRegistryDatabase.prototype.createObjectSchema = function(objectSchema) {
		return this.database.createEntity(objectSchema);
	};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
	 *            REQUIRED
	 * @return Promise that returns the elasticsearch response
	 * 
	 * returned object has the following properties: <code>
	 * cas			Couchbase CAS
	 * value		ObjectSchema object
	 * </code>
	 */
	ObjectSchemaRegistryDatabase.prototype.getObjectSchema = function(namespace, version) {
		var self = this;
		return when.promise(function(resolve, reject) {
			var id = objectSchemaId(namespace, version);
			when(self.database.getEntity(id), resolve, reject);
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
	 * @return Promise that returns an dictionary of :
	 * 
	 * <code>
	 * objectSchemaId -> {
	 * 						cas 			// Couchbase CAS
	 * 						value			// ObjectShema
	 *  				 }
	 * <code>
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
	ObjectSchemaRegistryDatabase.prototype.deleteObjectSchema = function(namespace, version) {
		var self = this;
		return when.promise(function(resolve, reject) {
			var id = objectSchemaId(namespace, version);
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

			when(self.database.findByField({
				field : 'namespace',
				value : namespace,
				returnFields : [ 'version' ]
			}), function(results) {
				var versions = lodash.foldl(results.hits.hits, function(versions, item) {
					versions.push(item.fields.version);
					return versions;
				}, []);

				if (versions.length < results.hits.total) {
					var promises = [];
					var from = results.hits.total;
					var maxPageSize = 200;
					var pageSize = (results.hits.total < maxPageSize) ? results.hits.total : maxPageSize;
					while (from < results.hits.total) {
						promises.push(when(self.database.findByField({
							field : 'namespace',
							value : namespace,
							returnFields : [ 'version' ],
							from : from,
							pageSize : pageSize
						}), function(results) {
							return lodash.foldl(results.hits.hits, function(versions, item) {
								versions.push(item.fields.version);
								return versions;
							}, []);
						}, function(error) {
							throw error;
						}));
						from += pageSize;
					}

					when(when.all(promises), function(results) {
						results.forEach(function(result) {
							values = values.concat(result);
						});
						resolve(values);
					}, reject);
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
		// TODO: use facets query
		throw new Error('NOT IMPLEMENTED');
	};

	ObjectSchemaRegistryDatabase.prototype.findObjectSchemasByField = function(params) {
		return this.database.findByField(params);
	};

	module.exports = ObjectSchemaRegistryDatabase;
}());
