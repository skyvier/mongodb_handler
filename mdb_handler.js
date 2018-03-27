/** 
 * A MongoDB handler module 
 * @module mongodb_handler
 * @author Joonas Laukka
*/

/* MongoDB Handler private properties */

var MongoClient = require('mongodb').MongoClient,
    Mongo = require('mongodb');
	 Server = require('mongodb').Server,
	 ObjectID = require('mongodb').ObjectID,
    Binary = require('mongodb').Binary,
    GridStore = require('mongodb').GridStore,
    Grid = require('mongodb').Grid,
    Code = require('mongodb').Code,
    GridStream = require('gridfs-stream');

var Validator = require('jsonschema').Validator;
var Async = require('async');
var Fs = require('fs');
var Stream = require('stream');
var Util = require('util');
var Circular = require('circular');

/* A JSON schema for the database configuration file */
var configSchema = {
   "id": "/Config",
   "type": "object",
   "properties": {
      "server_url": { "type": "string" },
      "server_port": { "type": "string" },
      "db_name": { "type": "string" },
      "collections": { "type": "object" }
   },
   "required": ["server_url", "db_name"]
};

/* A JSON schema for a database object */
var dbObjectSchema = {
   "id": "/DbObject",
   "type": "object",
   "properties": {
      "collection": { "type": "string" },
      "values" : { "type": "object" }
   }
};

var mongoUrl; // url of the database
var srvParam; // server parameters object
var dataBase;
var v = new Validator(); // JSON schema validator

var DEFAULT_CONFIG_PATH = 'config.json';

function checkValidity(param, schema) {
   var i;
   var report = v.validate(param, schema); 

   if(report.errors.length === 0) 
     return true;

   console.log(schema.id + " file is not valid: ");
   for(i = 0; i < report.errors.length; i++) {
      console.log(report.errors[i].message);
   }
   return false;
}

function openConfig(file) {
   var data, path;

   path = file || DEFAULT_CONFIG_PATH;

   try {
      data = Fs.readFileSync(path, 'utf8');
   } catch(err) {
      console.log(err);
      return false;
   }

   if(!data) {
      console.log("the configuration file doens't exist");
      return false;
   }

   srvParam = JSON.parse(data);
   if(!checkValidity(srvParam, configSchema))
      return false;

   return true;
}

function testServer() {
   if(mongoUrl === undefined) {
      console.log("mongoclient hasn't been initialised");
      return false;
   }

   MongoClient.connect(mongoUrl, function (err, db) {
      if(err) {
         errorMessage("mongoclient test", err);
         return;
      }

      db.close();
   });

   return true;
}

function errorMessage(entity, error) {
   var string = "there was an error with " + entity + ": " + error;
   if (winston) {
      winston.error(string);
   } else {
      console.log();
   }
}

function detectRegex(object) {
   var i, regex, obj, prop;

   var objs = Object.keys(object).filter(function (value) {
      return typeof object[value] === 'object';
   });

   if(objs.length === 0) {
      return;
   }

   for(i = 0; i < objs.length; i++) {
      obj = object[objs[i]];
      for(prop in obj) {
         if(prop === '$regex') {
            object[objs[i]] = new RegExp(obj[prop][0], obj[prop][1]);
         }   
      } 
   }
}

function accessDatabase(callback) {
   dataBase = dataBase || null;

   if (dataBase)
      return callback(null, dataBase);

   MongoClient.connect(mongoUrl, callback);
}

function accessCollection(collection, callback) {
   accessDatabase(function (err, db) {
      if(err) {
         errorMessage("mongoclient.connect", err);
         return callback(err, null, db);
      }

      db.collection(collection, function (err, col) {
         callback(err, col, db);
      });
   });
}

function getCollectionName(id, output) {
   var queryObj = { values: { _id: new ObjectID(id) } },
      result;

   if(typeof id === 'undefined' || !id) {
      output("id undefined");
   }

   accessDatabase(function (err, db) {
      if(err) {
         if(db)
            db.close();
         errorMessage("mongoclient.connect", err);
         return output(err);
      }

      db.collections(function (err, cols) {
         if(err) {
            db.close();
            errorMessage("db.collections", err);
            return output(err);
         }

         // TODO: doesnt seem to work (server output logs)
         Async.detect(cols, function (col, callback) {
            queryOperation(queryObj, col, { limit: 1 }, function (err, doc) {
               if(err) {
                  return callback(false);
               }

               result = doc.length > 0;
               callback(result);
            });
         }, function (result) {
            if(typeof result === 'undefined') {
               db.close();
               return output(null, null);
            }
               
            output(null, result.collectionName);   
         });
      });
   });
}

function updateOne(filter, update, options, callback) {
   if(!checkValidity(update, dbObjectSchema))
      callback("validity error");

   accessCollection(update.collection, function (err, collection, db) {
      if(err) {
         if(db)
            db.close();

         errorMessage("database collection", err);
         return callback(err);
      }

      detectRegex(update.values);
      collection.updateOne(filter, update.values, options, callback);
   });
}

function dbOperation(operation, options, object, callback) {
   if(!checkValidity(object, dbObjectSchema))
      callback("validity error");

   accessCollection(object.collection, function (err, col, db) {
      if(err) {
         if(db)
            db.close();

         errorMessage("database collection", err);
         return callback(err);
      }

      detectRegex(object.values);

      operation(object, col, options, function (err, doc) {
         if(err) {
            db.close();
            return callback(err); 
         }

         callback(null, doc);
      });
   });
}

function insertOperation(object, collection, options, callback) {
   collection.insert(object.values, options, function (err, doc) {
      if(err) {
         errorMessage("insert query", err);
         return callback(err);
      }

      callback(null, doc);
   });
}

function queryOperation(object, collection, options, callback) {
   collection.find(object.values, options).toArray(function(err, doc) {
      if(err) {
         errorMessage("find query", err);
         return callback(err);
      }

      callback(null, doc);
   });
}

function globalQueryOperation(objects, count, options, output) {
   var i, base, tasks = [];

   objects.forEach(function (obj) {
      tasks.push(function (callback) {
         dbOperation(queryOperation, options, obj, callback);
      });
   });

   Async.parallel(tasks, function (err, docs) {
      if(err)
         return output(err);

      if(!docs || docs.length === 0)
         return output("no result");

      base = docs[0];

      /* concat inner arrays to base */
      for(i = 1; i < docs.length; i++) {
         base = base.concat(docs[i]);
      }
      docs = base;
         
      if(typeof count === 'number')
         docs = docs.slice(0, count);

      /* Attach the source collection to the results */
      Async.each(docs, function (doc, callback) {
         getCollectionName(new ObjectID(doc._id), function (err, colName) {
            if(err) {
               return callback(err);
            }

            doc.collection = colName || 'none';
            callback(null);
         });
      }, function (err) {
         if(err) {
            return output(err);
         }

         return output(null, docs);
      });
   });
}

function parseJSONdots(json) {
   var obj, keys, i;

   if (typeof json === 'string') 
      obj = json;
   else 
      obj = JSON.stringify(json);

   try {
      obj = JSON.parse(obj);
      keys = Object.keys(obj);
   } catch (err) {
      errorMessage("parseJSONdots", err);
      return obj;
   }

   var build = function (base, structureLeft, value) {
      var current = structureLeft[0];

      if (structureLeft.length > 1) {
         base[current] = {};
         structureLeft.shift();
         build(base[current], structureLeft, value);
      } else {
         base[structureLeft[0]] = value;
      }
   };

   for (i = 0; i < keys.length; i++) {
      var splitted = keys[i].split('.');
      if (splitted.length > 1) {
         build(obj, splitted, obj[keys[i]]); 
         delete obj[keys[i]]; 
      } else if (typeof obj[keys[i]] === 'object') {
         obj[keys[i]] = parseJSONdots(obj[keys[i]]);
      }
   }

   return obj;
}

function insertFileFromData(name, meta, data, type, callback, safe) {
   var fileSize = data ? data.length : 0;

   if (!meta && !data) {
      return callback('nothing to insert in insertFileFromData');
   } else if (!name) {
      return callback('no name provided in insertFileFromData');
   }

   meta = parseJSONdots(meta);

   accessDatabase(function (err, db) {
      if(err) {
         if(db)
            db.close();

         errorMessage("database access", err);
         return callback(err);
      }

      gridStore = new GridStore(db, new ObjectID(), name, 'w', { content_type: type, metadata: meta });

      gridStore.open(function(err, gridStore) {
         if (err) {
            errorMessage("gridstore open", err);
            return callback(err);
         }

         if (!data) {
            gridStore.close(callback);
            return;
         }

         gridStore.write(data, function (err, doc) {
            if (err) {
               errorMessage("gridstore write", err);
               return callback(err);
            }

            gridStore.close(function (err, result) {
               if (err) {
                  errorMessage("gridstore close", err);
                  return callback(err);
               }

               /* check if the sizes match */
               if (typeof safe === 'boolean' && safe) {
                  GridStore.read(db, result._id, function(err, readData) {
                     if (err) {
                        errorMessage("gridstore read", err);
                        return callback(err);
                     }

                     if (fileSize !== readData.length) {
                        errorMessage("size mismatch");
                        return callback("size mismatch");
                     }

                     return callback(null, doc);
                  });  
               } else {
                  return callback(null, doc);
               }
            });
         });
      });
   });
}

function getFileInsertStream(name, meta, type, callback) {
   var writeStream;

   if (!meta) {
      return callback('nothing to insert in insertFileFromData');
   } else if (!name) {
      return callback('no name provided in insertFileFromData');
   }

   meta = parseJSONdots(meta);

   accessDatabase(function (err, db) {
      if(err) {
         if(db)
            db.close();

         return callback(err);
      }

      var gfs = GridStream(db, Mongo);
      var stream = gfs.createWriteStream({ filename: name, content_type: type, metadata: meta, mode: 'w+' });
      return callback(null, stream);
   });
}

/* MongoDB handler exports */

/**
 * @function closeDatabase
 *
 * The library keeps the database open
 * to make operations faster. Use this
 * function to manually shut down the
 * database when needed.
 */
exports.closeDatabase = function () {
   dataBase = dataBase || null;

   if (dataBase) {
      dataBase.close();
   }

};

/**
 * @function checkConfig
 * 
 * Function reads and saves the server configuration.
 * Returns false if the config file isn't appropriate.
 * @todo: make the error handling better
*/
exports.checkConfig = openConfig;

/**
 * @function init
 *
 * Initialises the database: reads configuration, tests it,
 * and uses the parameters to connect to the database.
 *
 * @param configFile {String} configurations file path
 * @returns {Boolean} false if anything goes wrong.
*/
exports.init = function (configFile) {
   if(!openConfig(configFile)) {
      srvParam = null;
      console.log("srvParam is corrupted");
      return false;
   }

   if(srvParam.server_port === undefined)
      srvParam.server_port = 27017; 

   mongoUrl = 'mongodb://' + srvParam.server_url + 
               ':' + srvParam.server_port + "/" + srvParam.db_name;

   if(!testServer())
      return false;

   accessDatabase(function (err, db) {
      if(!err) {
         dataBase = db;
      } else {
         return false;
      }
   });

   return true;
};

/**
 * @function queryGlobal
 *
 * Database query operation which searches through multiple collections.
 *
 * @param objects {Object[]} the database objects to be used
 * @param count {Number} the amount of docs returned
 * @param callback {Function} callback function (err, doc)
*/
exports.queryGlobal = function (objects, count, callback) {
   globalQueryOperation(objects, count, {}, callback);
};


/**
 * @function insertOrUpdate
 *
 * Database operation inserts file or updates it if it exists.
 *
 * @param query {Object} database query object
 * @param update {Object} the database object to be inserted
 * @param callback {Function} callback function (err, doc)
*/
exports.insertOrUpdate = function (query, update, callback) {
   updateOne(query, update, { upsert: true }, callback); 
};

/**
 * @function insert
 *
 * Database insert operation.
 *
 * @param object {Object} the database object to be used
 * @param callback {Function} callback function
*/
exports.insert = dbOperation.bind(null, insertOperation, { w:1 });


/**
 * @function queryOne
 *
 * Database findOne operation.
 *
 * @param object {Object} the database object to be used
 * @param callback {Function} callback function
*/
exports.queryOne = dbOperation.bind(null, queryOperation, { limit: 1 });

/**
 * @function query
 *
 * Database find() operation.
 *
 * @param options {Object} find() option object
 * @param object {Object} the database object to be used
 * @param callback {Function} callback function
*/
exports.query = dbOperation.bind(null, queryOperation);

/**
 * @function insertFromhMeta
 *
 * Database large file insert operation with
 * metadata and content-type.
 *
 * @param path {String} file path 
 * @param name {String} name of the file in the database
 * @param meta {Object} the metadata of the file
 * @param type {String} the type of the file (see GridFS content types)
 * @param callback {Function} callback function
*/
exports.insertFromPath = function(path, name, meta, type, callback) {
	var gridStore, fileSize;

	Fs.open(path, 'r', function (err, data) {
		if (err) {
         errorMessage("fs error", err);
         return callback(err);
		}

      insertFileFromData(name, meta, data, type, callback);
	});
};

/**
 * @function insertFileFromPath
 *
 * Database large file insert operation with no metadata.
 *
 * @param path {String} file path 
 * @param name {String} name of the file in the database
 * @param callback {Function} callback function
*/
exports.insertFileFromPath = function (path, name, callback) {
   module.exports.insertFileWithMeta(path, name, null, null, callback);   
};

/**
 * @function insertFileFromData
 *
 * Database large file insert operation with metadata and raw data.
 *
 * @param name {String} identifier in database (all sizes) 
 * @param meta {Object} metadata
 * @param data {Buffer} data buffer
 * @param type {String} type string
 * @param callback {Function} callback function
*/
exports.insertFileFromData = insertFileFromData;
exports.getFileInsertStream = getFileInsertStream;

/**
 * @function readFile
 *
 * Read a large file from the database.
 *
 * @param idSeed {String|Number} seed for the database id
 * @param callback {Function} callback function
*/
exports.readFile = function (name, callback) {
   var content = new Buffer(0);
   var contentLength = 0;

   module.exports.getFileReadStream(name, function (err, stream) {
      stream.on('error', function (err) {
         return callback(err);
      });

      stream.on('data', function (chunk) {
         contentLength += chunk.length;
         content = Buffer.concat([content, chunk], contentLength);
      });

      stream.on('end', function (chunk) {
         if (chunk) {
            contentLength += chunk.length;
            content = Buffer.concat([content, chunk], contentLength);
         }
      });

      stream.on('close', function () {
         return callback(null, content);
      });
   });
};

/**
 * @function getFileReadStream
 *
 * Read a large file from the database as a stream.
 *
 * @param idSeed {String|Number} seed for the database id
 * @param callback {Function} callback function
*/
exports.getFileReadStream = function (name, callback) {
	var gridStore;

   accessDatabase(function (err, db) {
      if(err) {
         if(db)
            db.close();

         errorMessage("database access", err);
         return callback(err);
      }

      var gfs = GridStream(db, Mongo);
      var stream = gfs.createReadStream({ filename: name });
      return callback(null, stream);
   });
};


/**
 * @function fileExists
 *
 * Checks if a file exists in the database.
 *
 * @param idSeed {String|Number} seed for the database id
 * @param callback {Function} callback function
 * @todo doesn't work
*/
exports.fileExists = function (idSeed, callback) {
   var id;

   if (typeof idSeed === 'string') {
      id = idSeed;
   } else {
      id = ObjectID(idSeed);
   }

   accessDatabase(function (err, db) {
      if(err) {
         if(db)
            db.close();

         errorMessage("database access", err);
         return callback(err);
      }

      GridStore.exist(db, id, callback);   
   });
};

/**
 * @function rmFile
 *
 * Removes a file from the database.
 *
 * @param idSeed {String|Number} seed for the database id
 * @param callback {Function} error callback function
 */
exports.rmFile = function (idSeed, callback) {
   var id;

   if (typeof idSeed === 'string') {
      id = idSeed;
   } else {
      id = ObjectID(idSeed);
   }

   accessDatabase(function (err, db) {
      if(err) {
         if(db)
            db.close();

         errorMessage("database access", err);
         return callback(err);
      }

      GridStore.unlink(db, id, function (err, gridStore) {
         callback(err);
      });
   });
};
