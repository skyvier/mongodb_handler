/** 
 * A MongoDB handler module 
 * @module mongodb_handler
 * @author Joonas Laukka
*/

/* MongoDB Handler private properties */

var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;
var ObjectID = require('mongodb').ObjectID;
var Validator = require('jsonschema').Validator;

var Async = require('async');

/* A JSON schema for the database configuration file */
var configSchema = {
   "id": "/Config",
   "type": "object",
   "properties": {
      "server_url": { "type": "string" },
      "server_port": { "type": "string" },
      "db_name": { "type": "string" }
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

function openConfig() {
   var fs = require('fs');
   var data;

   try {
      data = fs.readFileSync('config.json', 'utf8');
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
   console.log("there was an error with " + entity + ": " + error);
}

function detectRegex(object) {
   var i, regex, obj, prop;

   console.log("\nDetecting regular expressions...");

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
   if(dataBase)
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

      var opName = operation.name;
      console.log("\n### " + opName + " ###");
      console.log("doing an " + opName + ": db." + object.collection +
                  "." + opName + "(" + JSON.stringify(object.values, null, 4) + ")");
      console.log(options);
      
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

/* MongoDB handler exports */

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
 * @returns {Boolean} false if anything goes wrong.
*/
exports.init = function () {
   if(!openConfig()) {
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
         console.log("database connection has been established");
      } else {
         return false;
      }
   });

   console.log("the mongodb server " + mongoUrl + " is operational");
   return true;
};


/**
 * @function queryGlobal
 *
 * Database query operation which searches through multiple collections.
 *
 * @param objects {Object[]} the database objects to be used
 * @param count {Number} the amount of docs returned
 * @param output {Function} callback function (err, doc)
*/
exports.queryGlobal = function (objects, count, callback) {
   globalQueryOperation(objects, count, {}, callback);
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

