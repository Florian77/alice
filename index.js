

let connection = {
    mongoDb: {},
    dynamoDbDocClient: {}
};

function mongoDb(name = 'DEFAULT') {
    return connection.mongoDb[name].db(process.env[`ALICE_MONGODB_${name}_DB`]);
/*    return new Promise( async (resolve, reject) => {
        try {
            const db = (await mongoDbConnection(name)).db(process.env[`ALICE_MONGODB_${name}_DB`]);
            resolve(db);
        }
        catch (e) {
            reject(e);
        }
    });*/
}
module.exports.mongoDb = mongoDb;

async function mongoDbConnection(name = 'DEFAULT') {
    return new Promise( async (resolve, reject) => {
        if(!connection.mongoDb[name]) {
            connection.mongoDb[name] = new (require('mongodb').MongoClient)(process.env[`ALICE_MONGODB_${name}_URL`], {useNewUrlParser: true});
            try {
                await connection.mongoDb[name].connect();
                // console.log(`Alice: connection.mongoDb[${name}].connect()`);
            }
            catch (e) {
                connection.mongoDb[name] = false;
                reject(e);
            }
        }
        resolve(connection.mongoDb[name]);
    });
}
module.exports.mongoDbConnection = mongoDbConnection;


function dynamoDbDocClient(name = 'DEFAULT') {

    if(!connection.dynamoDbDocClient[name]) {

        const AWS = require('aws-sdk');
        const region = process.env[`ALICE_DYNAMODB_${name}_REGION`] || process.env[`AWS_REGION`] || false;
        /*if(region){
            AWS.config.update({region: region});
        }*/

        connection.dynamoDbDocClient[name] = new AWS.DynamoDB.DocumentClient({apiVersion: '2012-08-10', region: region});
    }

    return connection.dynamoDbDocClient[name];
}
module.exports.dynamoDbDocClient = dynamoDbDocClient;


function getStage(){

}


function cleanUp() {
    // Close all MongoDB connections
    for (let key in connection.mongoDb) {
        if(connection.mongoDb[key]) {
            try {
                // console.log(`Alice: connection.mongoDb[${key}].close()`);
                connection.mongoDb[key].close();
                connection.mongoDb[key] = false;
            }
            catch (e) {
                console.error(e);
            }
        }
    }
}
module.exports.cleanUp = cleanUp;

function cleanReturn(body, statusCode = 200, header = false) {
    cleanUp();
    return {
        statusCode,
        body,
    };
}
module.exports.cleanReturn = cleanReturn;

function cleanReturnStringify(body, statusCode = 200, header = false) {
    return cleanReturn(JSON.stringify(body), statusCode, header);
}
module.exports.cleanReturnStringify = cleanReturnStringify;

function taskStart() {

}
function taskError() {

}

function taskFinished() {

}

function importCsv() {

}
function importEbay() {

}

