const ftDev = require('ftws-node-dev-tools');


// ----------------------------------------------------------
//  Get Stage from ENV
// ----------------------------------------------------------
function getStage(){
    return process.env['ALICE_STAGE'];
}
module.exports.getStage = getStage;


// ----------------------------------------------------------
//  DEBUG Output: ON / OFF
// ----------------------------------------------------------
let debug = String(process.env['ALICE_DEBUG']).toLowerCase() === 'true' || false;

const setDebugOn = () => debug = true;
module.exports.setDebugOn = setDebugOn;
module.exports.debug = debug;

const debugOn = () => debug === true;
module.exports.debugOn = debugOn;

const setDebugOff = () => debug = false;
module.exports.setDebugOff = setDebugOff;

const debugOff = () => debug === false;
module.exports.debugOff = debugOff;

if(debugOn()) ftDev.logJsonString(debug, 'ENV::ALICE_DEBUG =');

// ----------------------------------------------------------
//  DB Connection Pool
// ----------------------------------------------------------
let connection = {
    mongoDb: {},
    dynamoDbDocClient: {}
};


// ----------------------------------------------------------
//  Mongo DB Connection
// ----------------------------------------------------------
const mongoDb = (name = 'DEFAULT') => {
    return connection.mongoDb[name].db(process.env[`ALICE_MONGODB_${name}_DB`]);
};
module.exports.mongoDb = mongoDb;

const mongoDbConnection = async (name = 'DEFAULT') => {
    return new Promise( async (resolve, reject) => {
        if(!connection.mongoDb[name]) {
            connection.mongoDb[name] = new (require('mongodb').MongoClient)(process.env[`ALICE_MONGODB_${name}_URL`], {useNewUrlParser: true});
            try {
                await connection.mongoDb[name].connect();
                if(debugOn()) ftDev.log(`Alice: connection.mongoDb[${name}].connect()`);
            }
            catch (e) {
                connection.mongoDb[name] = false;
                reject(e);
            }
        }
        resolve(connection.mongoDb[name]);
    });
};
module.exports.mongoDbConnection = mongoDbConnection;


// ----------------------------------------------------------
//  AWS -> dynamoDb -> DocClient
// ----------------------------------------------------------
const dynamoDbDocClient = (name = 'DEFAULT') => {

    if(!connection.dynamoDbDocClient[name]) {

        const AWS = require('aws-sdk');
        const region = process.env[`ALICE_DYNAMODB_${name}_REGION`] || process.env[`AWS_REGION`] || false;
        /*if(region){
            AWS.config.update({region: region});
        }*/

        connection.dynamoDbDocClient[name] = new AWS.DynamoDB.DocumentClient({apiVersion: '2012-08-10', region: region});
    }

    return connection.dynamoDbDocClient[name];
};
module.exports.dynamoDbDocClient = dynamoDbDocClient;

// TODO:D-DB  Wrapper für Table Config

// TODO: AWS S3 Client


// ----------------------------------------------------------
//  Cleanup
// ----------------------------------------------------------
const cleanUp = () => {
    // Close all MongoDB connections
    for (let key in connection.mongoDb) {
        if(connection.mongoDb[key]) {
            try {
                if(debugOn()) ftDev.log(`Alice: connection.mongoDb[${key}].close()`);
                connection.mongoDb[key].close();
                connection.mongoDb[key] = false;
            }
            catch (e) {
                console.error(e);
            }
        }
    }
};
module.exports.cleanUp = cleanUp;


// ----------------------------------------------------------
//  Return formats
// ----------------------------------------------------------
const cleanReturn = (body, statusCode = 200, headers = {}) => {
    cleanUp();
    /*if(headers){
        return {
            statusCode,
            body,
            headers
        };
    }*/

    return {
        statusCode,
        body,
        headers
    };
};
module.exports.cleanReturn = cleanReturn;

const cleanReturnJson = (body, statusCode = 200, headers = {}) => {
    return cleanReturn(JSON.stringify(body), statusCode, headers);
};
// Depricated !!!
module.exports.cleanReturnStringify = cleanReturnJson;
// New Name
module.exports.cleanReturnJson = cleanReturnJson;


const cleanReturnHtml = (body, statusCode = 200, headers = {}) => {
    // TODO: Merge headers object
    return cleanReturn(body, statusCode, {
        'Content-Type': 'text/html',
    });
};
module.exports.cleanReturnHtml = cleanReturnHtml;


// ----------------------------------------------------------
//  import CSV
// ----------------------------------------------------------
const importCsv = async (taskList) => {
    return Promise.all(taskList.map(taskItem => {
        return new Promise(async (resolve, reject) => {
            let logName;
            const counter = {
                befor: 0,
                delete: 0,
                insert: 0,
                download: 0
            };
            try {
                const request = require('request');
                const csv = require('csvtojson');
                const wrapArray = require('wrap-array');

                const {
                    url,
                    collectionName,
                    scope,
                    mapData,
                    csvOptions
                } = taskItem;
                const scopeFilter = (taskItem.scopeFilter && typeof taskItem.scopeFilter === "function")
                    ? taskItem.scopeFilter(scope)
                    : taskItem.scopeFilter
                ;

                logName = collectionName + ':' + scope;
                if(debugOn()) ftDev.logJsonString(scopeFilter, `scopeFilter:${logName}`);


                let importData = await csv(csvOptions)
                    .fromStream(
                        request.get(url)
                    )
                ;
                counter.download = importData.length;
                if(debugOn()) ftDev.log(url, 'rows:', counter.download);

                if (importData.length > 0) {
                    if(debugOn()) ftDev.log('start import:', scope);
                    const collection = await mongoDb().collection(collectionName);

                    counter.befor = await collection.countDocuments(scopeFilter);
                    if(debugOn()) ftDev.log('count', logName, ':', counter.befor);

                    const resultDelete = await collection.deleteMany(scopeFilter);
                    counter.delete = resultDelete.deletedCount;
                    if(debugOn()) ftDev.mongo.logDeleteMany(resultDelete, logName);

                    if (mapData) {
                        importData = importData.map(mapData(scope));
                    }

                    const resultInsert = await collection.insertMany(importData);
                    counter.insert = resultInsert.insertedCount;
                    if(debugOn()) ftDev.mongo.logInsertMany(resultInsert, logName);
                } else {
                    const msg = `empty download result [${url}]`;
                    return reject(`error:${logName}:[${msg}]`);
                }
            } catch (e) {
                if(debugOn()) ftDev.error(e.message);
                reject(`error:${logName}:[${e.message}]`);
            }
            resolve('success:' + logName + ':' + JSON.stringify(counter));
        });
    }));
};
module.exports.importCsv = importCsv;


// ----------------------------------------------------------
//  import XML
// ----------------------------------------------------------
const importXml = async (taskList) => {
    return Promise.all(taskList.map(taskItem => {
        return new Promise(async (resolve, reject) => {
            let logName;
            const counter = {
                befor: 0,
                delete: 0,
                insert: 0,
                download: 0
            };
            try {
                const wrapArray = require('wrap-array');
                const request = require('request-promise-native');
                const parseString = require('util').promisify(require('xml2js').parseString);

                const {
                    url,
                    collectionName,
                    scope,
                    mapData,
                    rowPath
                } = taskItem;
                const scopeFilter = (taskItem.scopeFilter && typeof taskItem.scopeFilter === "function")
                    ? taskItem.scopeFilter(scope)
                    : taskItem.scopeFilter
                ;

                logName = collectionName + ':' + scope;
                if(debugOn()) ftDev.logJsonString(scopeFilter, `scopeFilter:${logName}`);

                let importData = await parseString(
                    await request(url),
                    {
                        explicitArray: false
                    }
                );
                importData = wrapArray(importData.rows.row); // TODO: dynamic row path extraction: use rowPath var

                counter.download = importData.length;
                if(debugOn()) ftDev.log(url, 'rows:', counter.download);

                if (importData.length > 0) {
                    if(debugOn()) ftDev.log('start import:', scope);
                    const collection = await mongoDb().collection(collectionName);

                    counter.befor = await collection.countDocuments(scopeFilter);
                    if(debugOn()) ftDev.log('count', logName, ':', counter.befor);

                    const resultDelete = await collection.deleteMany(scopeFilter);
                    counter.delete = resultDelete.deletedCount;
                    if(debugOn()) ftDev.mongo.logDeleteMany(resultDelete, logName);

                    if (mapData) {
                        importData = importData.map(mapData(scope));
                    }

                    const resultInsert = await collection.insertMany(importData);
                    counter.insert = resultInsert.insertedCount;
                    if(debugOn()) ftDev.mongo.logInsertMany(resultInsert, logName);
                } else {
                    const msg = `empty download result [${url}]`;
                    return reject(`error:${logName}:[${msg}]`);
                }
            } catch (e) {
                if(debugOn()) ftDev.error(e.message);
                reject(`error:${logName}:[${e.message}]`);
            }
            resolve('success:' + logName + ':' + JSON.stringify(counter));
        });
    }));
};
module.exports.importXml = importXml;


// ----------------------------------------------------------
//  Easy Task log
// ----------------------------------------------------------
const logTaskStart = () => {};

const logTaskError = () => {};

const logTaskFinished = () => {};


// ----------------------------------------------------------
//  import Ebay Items
// ----------------------------------------------------------
const importEbay = () => {};

// -> Ebay Export