'use strict';

var chalk = require('chalk');
require('dotenv').config()
const {ObjectId} = require('mongodb');

exports.run = async (params) => {

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

        // get dates from parameters or default
        var startDate = '2020-12-01 00:00:00 GMT'
        var endDate = '2021-01-01 00:00:00 GMT'

        if (params) {
          var dates = JSON.parse(params.replace(/'/g, '"'))
          startDate = dates.startDate + " 00:00:00 GMT"
          endDate = dates.endDate + " 00:00:00 GMT"
        }
        console.log(chalk.cyan.yellow(`\t * Date range: ${startDate.split(' ')[0]} - ${endDate.split(' ')[0]}`))


         // aggregate by month and project
         let pipeline = [
          {
            '$match': {
              'date': {
                '$gte': new Date(startDate), 
                '$lt': new Date(endDate)
              }
            }
          }, {
            '$addFields': {
              'price': {
                '$round': [
                  {
                    '$multiply': [
                      '$quantity', 50.0
                    ]
                  }, 1
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
              'into': 'invoices', 
              'whenMatched': 'replace'
            }
          }
        ]
           
        console.log("\t - Running aggregation by project, month, year: ");

        // initializing Timer
        var start = process.hrtime();

        await collection.aggregate(pipeline).toArray() // limit(2). removed // , { allowDiskUse: true } removed
       
        // stopping time and loging to console
        var end = process.hrtime(start);
        console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));


      } finally {
        client.close();                
        }

        console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"));
}



