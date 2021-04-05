var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
'use strict';

exports.run = async () => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Re-connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const COLLECTION = 'timesheets'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)
   
    try {
        // 2. Runnign the queries
        console.log(chalk.cyan.bold("\n2. Querying time sheets:"))
        let query
        let projection
        let sort

        // query all, show only date
        query = {}
        projection = {date:1}

        console.log(" - Running query: " + JSON.stringify(query) + ", projection: " + JSON.stringify(projection));
        var result = await collection.find(query, { projection } ).limit(5).toArray()
        console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(result, null, 2)))

        // query by quantity > 7 and sort descendent
        query = { quantity: { "$gt": 7 }}
        projection = { quantity: 1 }
        sort = { quantity: -1 }

        console.log(" - Running query: " + JSON.stringify(query) 
                     + ", projection: " + JSON.stringify(projection) 
                     + ", sort: " + JSON.stringify(sort));
        var result = await collection.find(query, { projection } ).sort(sort).limit(5).toArray()
        console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(result, null, 2)))

        // 3. Runnign an aggregation
        console.log(chalk.cyan.bold("\n3. Aggregating time sheets:"))

        // aggregate by project
        let match = {
              '$match': {
                'date': {
                  '$gte': new Date('Fri, 01 Jan 2021 00:00:00 GMT'), 
                  '$lt': new Date('Mon, 01 Feb 2021 00:00:00 GMT')
                }
              }
            }
        
        let addFieldsPrice = {
              '$addFields': {
                'price': {
                  '$round': [
                    {
                      '$multiply': [
                        '$quantity', 63.0
                      ]
                    }, 1
                  ]
                }
              }
            }
            
        let groupByProject = {
              '$group': {
                '_id': {
                  'project': '$ref-project'
                }, 
                'count': {
                  '$sum': 1
                }, 
                'amount': {
                  '$sum': '$price'
                }, 
                'item': {
                  '$push': {
                    'ref-timesheet': '$_id', 
                    'quantity': '$quantity', 
                    'price': '$price'
                  }
                }
              }
            }
        
        // you can define by steps as above or copy-paste from compass directly the whole pipeline into pipeline
        let pipeline = [match, addFieldsPrice, groupByProject]
        console.log(" - Running aggregation by project: ");
        var aggregationResult = await collection.aggregate(pipeline).limit(2).toArray()
        console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2)))
        
    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"))
}
