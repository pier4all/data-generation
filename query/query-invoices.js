var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
'use strict';

exports.run = async () => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Re-connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const COLLECTION = 'invoices'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)
   
    try {
      
        // 2. Running an aggregation
        console.log(chalk.cyan.bold("\n2. Aggregating Top 10 Projects:"))

                 let pipeline = [
                  {
                    '$group': {
                      '_id': '$_id.ref-project', 
                      'hours': {
                        '$sum': '$hours'
                      }, 
                      'amount': {
                        '$sum': '$amount'
                      }
                    }
                  }, {
                    '$sort': {
                      'amount': -1
                    }
                  }, {
                    '$limit': 10
                  }, {
                    '$lookup': {
                      'from': 'projects', 
                      'localField': '_id', 
                      'foreignField': '_id', 
                      'as': 'project'
                    }
                  }, {
                    '$lookup': {
                      'from': 'employees', 
                      'localField': 'project.ref-employee', 
                      'foreignField': '_id', 
                      'as': 'employee'
                    }
                  }, {
                    '$addFields': {
                      'project_name': {
                        '$first': '$project.title'
                      }, 
                      'project_manager': {
                        '$first': '$employee.lastname'
                      }
                    }
                  }, {
                    '$project': {
                      'project_name': 1, 
                      'project_manager': 1, 
                      'service_hours': '$hours', 
                      'totally_invoiced': '$amount'
                    }
                  }
                ]  
                    
                console.log(" - Running aggregation: ");
        
                // initializing timer
                var start = process.hrtime();
        
                await collection.aggregate(pipeline).toArray() 

                // stopping time
                var end = process.hrtime(start);

                // documenting results
                var aggregationResult = await collection.aggregate(pipeline).toArray() 
                console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2))) 
               
                // printing execution time
                console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));

        // 3. Running an aggregation
        console.log(chalk.cyan.bold("\n3. Aggregating Sales by Month:"))

                 let pipeline2 = [
                  {
                    '$group': {
                      '_id': '$_id.period', 
                      'hours': {
                        '$sum': '$hours'
                      }, 
                      'amount': {
                        '$sum': '$amount'
                      }
                    }
                  }, {
                    '$sort': {
                      '_id': -1
                    }
                  }, {
                    '$project': {
                      'period': '$period', 
                      'service_hours': '$hours', 
                      'totally_invoiced': '$amount'
                    }
                  }
                ]
                    
                console.log(" - Running aggregation: ");
        
                // initializing timer
                var start = process.hrtime();
        
                await collection.aggregate(pipeline2).toArray() 

                // stopping time
                var end = process.hrtime(start);

                // documenting results
                var aggregationResult = await collection.aggregate(pipeline2).toArray() 
                console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2))) 
               
                // printing execution time
                console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));

        // 4. Running an aggregation
        console.log(chalk.cyan.bold("\n4. Aggregating Sales by Customer and Month:"))

                 let pipeline3 = [
                  {
                    '$match': {
                      '_id.period': '2020-12' // specfify period in the format yyyy-mm
                    }
                  }, {
                    '$lookup': {
                      'from': 'projects', 
                      'localField': '_id.ref-project', 
                      'foreignField': '_id', 
                      'as': 'project'
                    }
                  }, {
                    '$lookup': {
                      'from': 'customers', 
                      'localField': 'project.ref-customer', 
                      'foreignField': '_id', 
                      'as': 'customer'
                    }
                  }, {
                    '$addFields': {
                      'customer_name': {
                        '$first': '$customer.name'
                      }
                    }
                  }, {
                    '$group': {
                      '_id': '$customer_name', 
                      'hours': {
                        '$sum': '$hours'
                      }, 
                      'amount': {
                        '$sum': '$amount'
                      }
                    }
                  }, {
                    '$sort': {
                      'amount': -1
                    }
                  }, {
                    '$limit': 10
                  }, {
                    '$project': {
                      'service_hours': '$hours', 
                      'totally_invoiced': '$amount'
                    }
                  }
                ]
                    
                console.log(" - Running aggregation: ");
        
                // initializing timer
                var start = process.hrtime();
        
                await collection.aggregate(pipeline3).toArray() 

                // stopping time
                var end = process.hrtime(start);

                // documenting results
                var aggregationResult = await collection.aggregate(pipeline3).toArray() 
                console.log(chalk.yellow.italic(" * Result: " + JSON.stringify(aggregationResult, null, 2))) 
               
                // printing execution time
                console.log(chalk.magenta.bold(`\t  Execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));

    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"))
}
