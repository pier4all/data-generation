var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
'use strict';

exports.run = async () => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Re-connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const INVOICES = 'invoices3'
    const TIMESHEETS = 'timesheets'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const invoices = db.collection(INVOICES)
    const timesheets = db.collection(TIMESHEETS)
   
    try {
      
        // 2. Finding billed timesheets
        console.log(chalk.cyan.bold("\n2. Finding billed timesheets:"))

                 let billed_ids_pipeline = [
                  {
                    '$project': {
                      '_id': 0, 
                      'item.ref-timesheet': 1
                    }
                  }, {
                    '$unwind': {
                      'path': '$item', 
                      'includeArrayIndex': 'string', 
                      'preserveNullAndEmptyArrays': false
                    }
                  }, {
                    '$replaceRoot': {
                      'newRoot': '$item'
                    }
                  }, {
                    '$limit': 10
                  }
                ]
                    
                console.log(" - Running aggregation: ");
        
                // initializing timer
                var start = process.hrtime();
        
                var aggregationResult = await invoices.aggregate(billed_ids_pipeline).toArray() 

                // stopping time
                var end = process.hrtime(start);

                // documenting results
                console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2)))
               
                // printing execution time
                console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));

                var timesheet_id_array = aggregationResult.map(document => document["ref-timesheet"])


        // 3. Updating timesheets
        console.log(chalk.cyan.bold("\n3. Updating timesheets:"))

              let query = { _id: { $in: timesheet_id_array }, billed: { $ne: true} };
              let update = { $set: { billed: true } };
              
              console.log(" - Running aggregation: ");

              // initializing timer
              var start = process.hrtime();

              const result = await timesheets.updateMany(query, update)

              // stopping time
              var end = process.hrtime(start);

              // documenting results
              console.log(chalk.yellow.italic(" * Result: " + result))
              
              // printing execution time
              console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));
                
    } catch (error) {
        console.log(error)
        console.log(chalk.red.bold(`\t  ERROR: ${error.message}\n`));
              
    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"))
}
