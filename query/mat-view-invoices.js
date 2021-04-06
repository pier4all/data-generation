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
        // 2. Running an Aggregation (mat-view)
        console.log(chalk.cyan.bold("\n2. Aggregating time sheets by project and month:"))

         // aggregate by month and project
         let pipeline = [
          {
            '$match': {
              'date': {
                '$gte': new Date('01 Dec 2020 00:00:00 GMT'), 
                '$lt': new Date('01 Jan 2021 00:00:00 GMT')
              }
            }
          }, {
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
                'period': {
                  '$dateToString': {
                    'format': '%Y-%m', 
                    'date': '$date'
                  }
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
              'hours_booked': {
                '$sum': '$quantity'
              }, 
              'amount': {
                '$sum': '$price'
              }
            }
          }, {
            '$merge': {
              'into': 'monthlyInvoicing', 
              'whenMatched': 'replace'
            }
          }
        ]
           
        console.log(" - Running aggregation by project, month, year: ");

        // initializing Timer
        var start = process.hrtime();
        console.log(start); // timer start


        var aggregationResult = await collection.aggregate(pipeline).toArray() // limit(2). removed // , { allowDiskUse: true } removed
        console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2)))       
       
        // stopping time and loging to console
        var end = process.hrtime(start);
        console.log(chalk.bgRed.bold(`\nExecution time: ${end[0]}s ${end[1] / 1000000}ms\n`));


      } finally {
        client.close();                
        }

        console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"));
}



