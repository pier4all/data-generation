'use strict';

var chalk = require('chalk');
require('dotenv').config()
const {ObjectId} = require('mongodb');

exports.run = async () => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Connecting to MongoDB:"))

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
        // 4. Running an Aggregation Test 2
        console.log(chalk.cyan.bold("\n4. Aggregating time sheets by project and month:"))

         // aggregate by project and month (limited by one month - due to memory failure)
         let pipeline = [     
          {
              '$addFields': {
                'price': {
                  '$multiply': [
                    '$quantity', 50
                  ]
                }
              }
            }, {
              '$group': {
                '_id': {
                  'project': '$ref-project', 
                  'year': {
                    '$year': '$date'
                  }, 
                  'month': {
                    '$month': '$date'
                  }
                }, 
                'item': {
                  '$push': {
                    'ref-timesheet': '$_id', 
                    'quantity': '$quantity', 
                    'price': '$price', 
                    'date': '$date'
                  }
                }, 
                'amount': {
                  '$sum': '$price'
                }
              }
            }
        ]
        console.log(" - Running aggregation by project, month, year: ");

        // initializing Timer
        var start = process.hrtime();
        console.log(start); // timer start


        var aggregationResult = await collection.aggregate(pipeline, { allowDiskUse: true }).limit(2).toArray()
        console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2)))       
       
        // stopping time and loging to console
        var end = process.hrtime(start);
        console.log(chalk.bgRed.bold(`\nExecution time: ${end[0]}s ${end[1] / 1000000}ms\n`));


      } finally {
        client.close();                
        }

        console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"));
}



