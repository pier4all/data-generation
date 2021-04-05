var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
'use strict';
const fs = require('fs');

exports.run = async () => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Re-connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const COLLECTION = 'timesheets'
    const REF_COLLECTION = 'projects'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)
    const refCollection = db.collection(REF_COLLECTION)
   
    try {
        // 2. Runnign the queries
        console.log(chalk.cyan.bold("\n1. Querying project id list:"))
        let query
        let projection
        let sort

        // query all, show only date
        query = {}
        projection = {_id:1}

        console.log(" - Running query: " + JSON.stringify(query) + ", projection: " + JSON.stringify(projection));
        var projects = await refCollection.find(query, { projection } ).toArray()
        console.log(chalk.yellow.italic(" * Result: " + projects.length + " projects"))

        // 2. Running an aggregation iteratively
        console.log(chalk.cyan.bold("\n3. Aggregating time sheets:"))

        var outputPath = './data/output/billing.json'
        fs.writeFileSync(outputPath, '[\n')

        for (let project of projects) {
          // aggregate by project
          let match = {
            '$match': {
                'ref-project': ObjectID(project._id)
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
          
          // you can define by steps as above or copy-paste from compass directly the whole pipeline into pipeline
          let pipeline = [match, addFieldsPrice, groupByProject]
          console.log(" - Running aggregation by project: ");
          var aggregationResult = await collection.aggregate(pipeline).toArray()
          console.log(chalk.yellow.italic(" * Result: project " + project._id + " bills: " + aggregationResult.length))
          
          // remove list signs 
          var billingStr = JSON.stringify(aggregationResult, null, 2).replace('[', '').split("")
          billingStr[billingStr.lastIndexOf(']')] = ',';
          if (aggregationResult.length>0) fs.appendFileSync(outputPath, billingStr.join(""))
      }

      fs.appendFileSync(outputPath, '\n]\n')
      console.log(chalk.yellow.italic("* Saved billing to: " + outputPath))
        
    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"))
}
