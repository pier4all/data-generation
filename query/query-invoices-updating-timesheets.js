var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
'use strict';

exports.run = async () => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Re-connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const INVOICES = 'invoices'
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

                 let billed_ids = [
                  {
                    '$project': {
                      '_id': 0, 
                      'item.ref-timesheet': 1
                    }
                  }, {
                    '$limit': 10
                  }
                ]
                    
                console.log(" - Running aggregation: ");
        
                // initializing timer
                var start = process.hrtime();
        
                await invoices.aggregate(billed_ids).toArray() 

                // stopping time
                var end = process.hrtime(start);

                // documenting results
                var aggregationResult = await invoices.aggregate(billed_ids).toArray() 
                console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2)))
               
                // printing execution time
                console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));


        // 3. Updating timesheets
        console.log(chalk.cyan.bold("\n3. Updating timesheets:"))

                let query = 
                  { _id: { $in: billed_ids } },
                  { $set: { billed: true } }
              
                  
              console.log(" - Running aggregation: ");

              // initializing timer
              var start = process.hrtime();

              await timesheets.updateMany(query).toArray() 

              // stopping time
              var end = process.hrtime(start);

              // documenting results
              var aggregationResult = await timesheets.updateMany(query).toArray() 
              console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2)))
              
              // printing execution time
              console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));
                

    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"))
}
