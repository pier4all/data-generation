'use strict';

var faker = require('faker');
var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
const fs = require('fs');
var path = require('path');
const { getRandomInt } = require("./common/util")

exports.run = async (numDocuments = 20) => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n2. Connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const COLLECTION = 'timesheets'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)

    // 2. query the referenced collections
    console.log(chalk.cyan.bold("\n2. Query referenced collections:"))
    var projection = {_id:1}

    var refcollection = db.collection("customers")
    console.log(" - Running query on " + refcollection.namespace + ", projection: " + JSON.stringify(projection));
    var customers = await refcollection.find({}, { projection } ).toArray()
    console.log(" - Read " + customers.length + " " + refcollection.namespace.split('.')[1]);

    refcollection = db.collection("employees")
    console.log(" - Running query on " + refcollection.namespace + ", projection: " + JSON.stringify(projection));
    var employees = await refcollection.find({}, { projection } ).toArray()
    console.log(" - Read " + employees.length + " " + refcollection.namespace.split('.')[1]);
 
    refcollection = db.collection("services")
    console.log(" - Running query on " + refcollection.namespace + ", projection: " + JSON.stringify(projection));
    var services = await refcollection.find({}, { projection } ).toArray()
    console.log(" - Read " + services.length + " " + refcollection.namespace.split('.')[1]);

    refcollection = db.collection("projects")    
    var projection = {"ref-employee":1}
    console.log(" - Running query on " + refcollection.namespace + ", projection: " + JSON.stringify(projection));
    var projects = await refcollection.find({}, { projection } ).toArray()
    console.log(" - Read " + projects.length + " " + refcollection.namespace.split('.')[1]);

   // 3. Generate timesheets and WRITE them in batches to files
    console.log(chalk.cyan.bold("\n3. Generating time sheets:"))

    // TODO: Guess all this params from numDocuments (original 1000000)
    const minDates = Math.min(5, numDocuments)	  // minimum amount of documents per input combination
    const maxDates = Math.min(30, numDocuments)    // minimum amount of documents per input combination
    const batchSize = (numDocuments>500) ? Math.max(1, numDocuments/10) : numDocuments // number of documents to store in each JSON file 
   
    var timesheet; 
    var timesheets = []; // timesheets as Javascript objects to import into Mongo

    var timesheet_export;
    var timesheets_export = []; // timesheets to save to file with proper formatting

    var count = 0
    var outputFiles = []

    // do the insertions
    try {

        while ((count + timesheets.length) < numDocuments) { 
            var projectDoc = projects[getRandomInt(0, projects.length)]
            var project = projectDoc._id
            var employee = employees[getRandomInt(0, employees.length)]._id
            var service = services[getRandomInt(0, services.length)]._id

            var numDates = getRandomInt(minDates, maxDates)
            for (let l = 0; l < numDates; l++) { 
                // TODO: check not repeating
                // TODO: limit period

                // toggle comment on next line to set it to date with zeroes or string
                var date = new Date(faker.date.past().toISOString().slice(0,10));
                //var date = faker.date.past().toISOString().slice(0,10);

                // Random float number between 8.9 and 1 
                var quantity = 1.0 + (faker.random.number() % 8) + (faker.random.number() % 10) / 10.0
                
                timesheet = { 
                    "ref-project": new ObjectID(project) , 
                    "ref-employee": new ObjectID(employee),
                    "ref-service": new ObjectID(service), 
                    date: date,
                    quantity
                }    

                timesheet_export = { 
                    "ref-project": { "$oid": new ObjectID(project) }, 
                    "ref-employee": { "$oid": new ObjectID(employee) },
                    "ref-service": { "$oid": new ObjectID(service) }, 
                    date: { "$date": date },
                    quantity
                }    

                timesheets.push(timesheet)
                timesheets_export.push(timesheet_export)

                // generate (sometimes) one timesheet entry for the project-assigned employee on that date
                if (((count + timesheets.length) < numDocuments) && (timesheets.length < batchSize)){
                    quantity = 1.0 + (faker.random.number() % 8) + (faker.random.number() % 10) / 10.0
                    if (quantity < 3) {

                        timesheet = { 
                            "ref-project": new ObjectID(project) , 
                            "ref-employee": new ObjectID(projectDoc["ref-employee"]),
                            "ref-service": new ObjectID(service), 
                            date: date,
                            quantity
                        }    
        
                        timesheet_export = { 
                            "ref-project": { "$oid": new ObjectID(project) }, 
                            "ref-employee": new ObjectID(projectDoc["ref-employee"]),
                            "ref-service": { "$oid": new ObjectID(service) }, 
                            date: { "$date": date },
                            quantity
                        }  

                        timesheets.push(timesheet)
                        timesheets_export.push(timesheet_export)
                    }
                }

                // save one batch to file and db, end the process if enough time sheets
                if ((timesheets.length >= batchSize) || ((count + timesheets.length) >= numDocuments)){
                    count += timesheets.length

                    var outputPath = path.join(__dirname, '..', 'data', 'output', 'timesheet_' + count + '.json');
                    fs.writeFileSync(outputPath, JSON.stringify(timesheets_export, null, 2))
                    outputFiles.push(outputPath)
                    timesheets_export = []
                    console.log(chalk.yellow.italic("* Saved batch to: " + outputPath))

                    console.log(chalk.yellow.italic("* Inserting batch to MongoDB: " + collection.namespace))

                    // Timer
                    var start = process.hrtime();
                    console.log(chalk.magenta("\t - Timing insertMany of " + timesheets.length + " documents in MongoDB...")); // timer start

                    await collection.insertMany(timesheets)
                    timesheets = []

                    // stopping time and loging to console
                    var end = process.hrtime(start);
                    console.log(chalk.magenta.bold(`\t\t ... execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));

                    //exit loop if enough total timesheets
                    if (count >= numDocuments) break
                }
            }
            if (count < numDocuments) console.log("- Generated \t" + (timesheets.length + count) + "/" + numDocuments)
        }

    } finally {
        client.close();                
    }

}
