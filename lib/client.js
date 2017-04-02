/*
 * This file is part of the bass-redis library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

const QueryResult = require('../../bass').QueryResult;

module.exports = class Client {

	/**
	 * Lua script to be able to run queries in redis
	 * 
	 * @return {String}
	 */
	static get LUA_CRITERIA_SCRIPT() {
		
		return `

			function table.slice(tbl, first, last, step)
			    local sliced = {}

			    for i = first or 1, last or #tbl, step or 1 do
			      sliced[#sliced + 1] = tbl[i]
			    end

			    return sliced
			end

			-- function to see if a table contains a value
			local function contains(table, val)
			   for i = 1, #table do
			      if table[i] == val then 
			         return true
			      end
			   end
			   return false
			end

			-- get arguments
			local keyPattern = ARGV[1]
			local criteria = cjson.decode(ARGV[2])
			local sort = cjson.decode(ARGV[3])
			local skip = ARGV[4]
			local limit = ARGV[5]

			-- get all matching keys
			local keys = redis.call('keys', keyPattern)
			
			local results = {}
			local currentKey = nil
			local item

			for index, key in pairs(keys) do

				-- load hash set
				local data = redis.call('hgetall', key)
				item = {}

				local isMatch = true
				local x = 0

				-- add key value pairs to result object
				for i, value in pairs(data) do

					-- grab the value
					if x%2 == 0 then
						currentKey = value
					else

						-- check if condition exists for key
						if criteria[currentKey] then

							-- do equality check: {"foo":"bar"}
							if type(criteria[currentKey]) == 'string' and criteria[currentKey] ~= value then
								isMatch = false
								break
							end

							-- do "in" check: {"$in":[1,2,3]} 
							if type(criteria[currentKey]) == 'table' and criteria[currentKey]['$in'] and not contains(criteria[currentKey]['$in'], value) then
								isMatch = false
								break
							end

						end

						-- set the key/value
						item[currentKey] = value
					end

					x = x + 1
				end

				-- add object to result
				if isMatch == true then
					table.insert(results, item)
				end
			end

			-- get total results
			local total = table.getn(results)


			-- check if table needs to be sorted
			if sort then

				for key, value in pairs(sort) do

					if value == 1 then
						table.sort(results, function(a, b) return a[key] < b[key] end)
					else
						table.sort(results, function(a, b) return a[key] > b[key] end)
					end

				end

			end

			-- slice the table
			if skip ~= "null" and limit ~= "null" then
				results = table.slice(results, skip+1, skip+limit)
			end

			return cjson.encode({ data = results, total = total })
		`;
	}

	/**
	 * Lua script to be able to run queries in redis
	 * 
	 * @return {String}
	 */
	static get LUA_DROP_COLLECTION_SCRIPT() {
		return `


		`;
	}

	constructor(db, logger) {
		this.db = db;
		this.logger = logger;
	}

	/**
	 * Generate a new key for the database/collection/id combination
	 * 
	 * @param  {String} collection
	 * @param  {String} id
	 * @return {String}
	 */
	generateNewKey(collection, id) {
		return this.db.database + ':' + collection + ':' + id;
	}

	/**
	 * Insert a new document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	insert(metadata, collection, data, cb) {

		const start = new Date();

		// see if an id was manually generated
		let id = data[metadata.getIdFieldName()];

		// generate a new id if there isn't one
		if (id === null || typeof id === 'undefined') {
			id = this.db.generateIdFieldValue();
		}

		data[metadata.getIdFieldName()] = id;
		
		const key = this.generateNewKey(collection, id);

		this.db.connection.hmset(key, data, (err, res) => {
			this.logger.debug('[bass-redis] - insert [' + collection + ']: ' + JSON.stringify(data) + ' : ' + (new Date() - start) + 'ms');
			cb(err, data);
		});
	}

	/**
	 * Update a document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	update(metadata, collection, id, data, cb) {

		const start = new Date();
		const key = this.generateNewKey(collection, id);

		this.db.connection.hmset(key, data, (err, res) => {
			this.logger.debug('[bass-redis] - remove [' + collection + ': ' + id + ' : ' + (new Date() - start) + 'ms');
			cb(err, data);
		});
	}

	/**
	 * Remove a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove(metadata, collection, id, cb) {

		const start = new Date();
		const key = this.generateNewKey(collection, id);

		this.db.connection.del(key, (err, data) => {
			this.logger.debug('[bass-redis] - remove [' + collection + ': ' + id + ' : ' + (new Date() - start) + 'ms');
			cb(err);
		});
	}

	/**
	 * Find a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find(metadata, collection, id, cb) {

		const start = new Date();
		const key = this.generateNewKey(collection, id);

		this.db.connection.hgetall(key, (err, item) => {
			this.logger.debug('[bass-redis] - find [' + collection + ']: ' + id + ' : ' + (new Date() - start) + 'ms');
			console.log(item);
			cb(err, item);
		});			
	}

	/**
	 * Find documents based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findByQuery(metadata, collection, query, cb) {

		const start = new Date();
		const criteria = this.convertQueryToCriteria(query);
		const keyPattern = this.db.database + ':' + collection + ':*';

		// fix up null values
		const sort = query.getSort() || {};
		const skip = query.getSkip() || "null";
		const limit = query.getLimit() || "null";


		this.db.connection.eval(Client.LUA_CRITERIA_SCRIPT, 0, keyPattern, JSON.stringify(criteria), JSON.stringify(query.getSort()), skip, limit, (err, data) => {

			try {

				const queryResult = new QueryResult(query);
				const results = JSON.parse(data);

				// fix data if it's an empty object
				if (typeof results.data === 'object' && Object.keys(results.data).length === 0) {
					results.data = [];
				}

				queryResult.setData(results.data);

				if (query.getCountFoundRows()){
					queryResult.totalRows = results.total;
				}

				this.logger.debug('[bass-redis] - findByQuery [' + collection + ']: ' + JSON.stringify(query) + ' : ' + (new Date() - start) + 'ms');

				cb(null, queryResult);

			} catch (e) {

				this.logger.error("-----------------------");
				this.logger.error(err);
				this.logger.error(data);
				cb(e);
			}
		});
	}

	/**
	 * Get a document count based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findCountByQuery(metadata, collection, query, cb) {

		cb(0);

		// const mongoCriteria = this.convertQueryToCriteria(query);

		// this.db.collection(collection, function(err, coll) {
		// 	const cursor = coll.count(mongoCriteria, function(err, count) {
		// 		cb(err, count);
		// 	});
		// });
	}

	/**
	 * Find documents by simple criteria
	 * 
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Object}    sort
	 * @param  {Number}    skip
	 * @param  {Number}    limit
	 * @param  {Function}  cb
	 * @return {void}
	 */
	findBy(metadata, collection, criteria, sort, skip, limit, cb) {

		const start = new Date();
		const keyPattern = this.db.database + ':' + collection + ':*';

		// make sure we have some sort of criteria object
		if (typeof criteria === 'undefined') {
			criteria = {};
		}

		// convert empty values to string: "null"
		if (typeof sort === 'undefined' || sort == null) {
			sort = {};
		}

		if (typeof skip === 'undefined' || skip == null) {
			skip = "null";
		}

		if (typeof limit === 'undefined' || limit == null) {
			limit = "null";
		}	

		this.db.connection.eval(Client.LUA_CRITERIA_SCRIPT, 0, keyPattern, JSON.stringify(criteria), JSON.stringify(sort), skip, limit, (err, data) => {

			data = JSON.parse(data);

			try {

				// fix data if it's an empty object (empty "array" in lua)
				if (typeof data.data === 'object' && Object.keys(data.data).length === 0) {
					data = [];
				}

				this.logger.debug('[bass-redis] - findByQuery [' + collection + ']: ' + JSON.stringify(criteria) + ' : ' + (new Date() - start) + 'ms');

				cb(null, data.data);

			} catch (e) {

				this.logger.error("-----------------------");
				this.logger.error(err);
				this.logger.error(data);
				cb(e);
			}
		});
	}

	/**
	 * Find documents where a field has a value in an array of values
	 *
	 * @param {Metadata} metadata The metadata for the document type you are fetching
	 * @param {String} field The document's field to search by
	 * @param {Array.<(String|Number)>} values Array of values to search for
	 * @param {Object|null} sort Object hash of field names to sort by, -1 value means DESC, otherwise ASC
	 * @param {Number|null} limit The limit to restrict results
	 * @param {Function} cb Callback function
	 */
	findWhereIn(metadata, field, values, sort, limit, cb) {

		const criteria = {};
		criteria[field] = {'$in' : values};

		this.findBy(
			metadata,
			metadata.collection,
			criteria ,
			sort,
			null,
			limit || undefined,
			cb
		);
	}

	/**
	 * Create a collection
	 * 
	 * @param  {[type]}   metadata   [description]
	 * @param  {[type]}   collection [description]
	 * @param  {Function} cb         [description]
	 * @return {[type]}              [description]
	 */
	create(metadata, collection, cb) {
		cb(null);
	}

	/**
	 * Drop a collection
	 * 
	 * @param  {String}   collection
	 * @param  {Function} cb
	 * @return {void}
	 */
	drop(metadata, collection, cb) {
		this.db.collection(collection, function(err, coll) {
			coll.drop(cb);
		});
	}

	/**
	 * Rename a collection
	 * 
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {String}    newName
	 * @param  {Function}  cb
	 * @return {void}
	 */
	rename(metadata, collection, newName, cb) {
		this.db.collection(collection, function(err, coll) {
			coll.rename(newName, cb);
		});
	}

	/**
	 * Get a list of all of the collection names in the current database
	 * 
	 * @param  {Function} cb
	 * @return {void}
	 */
	listCollections(cb) {
		this.db.collections(cb);
	}

	/**
	 * Convert a Bass Query to MongoDB criteria format
	 * 
	 * @param  {Query} query
	 * @return {Object}
	 */
	convertQueryToCriteria(query) {

		const newQuery = {};
		const conditions = query.getConditions();

		let field, tmp, i;

		for (field in conditions){

			if (typeof conditions[field] === 'object'){

				tmp = {};

				for (i in conditions[field]){
					tmp['$' + i] = conditions[field][i];
				}

				newQuery[field] = tmp;

			} else {
				newQuery[field] = conditions[field];
			}
		}

		return newQuery;
	}

}
