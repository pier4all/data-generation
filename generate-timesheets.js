'use strict';

var faker = require('faker');
var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
const fs = require('fs');

exports.run = async () => {

    // 1. READ input files
    console.log(chalk.cyan.bold("\n1. Reading input files:"))

    var rawdata = fs.readFileSync('./data/input/customer.json');
    let customers = JSON.parse(rawdata);
    console.log(" - Read " + customers.length + " customers");

    rawdata = fs.readFileSync('./data/input/employee.json');
    let employees = JSON.parse(rawdata);
    console.log(" - Read " + employees.length + " employees");

    rawdata = fs.readFileSync('./data/input/project.json');
    let projects = JSON.parse(rawdata);
    console.log(" - Read " + projects.length + " projects");

    rawdata = fs.readFileSync('./data/input/service.json');
    let services = JSON.parse(rawdata);
    console.log(" - Read " + services.length + " services");

    // 2. Connect to the db
    console.log(chalk.cyan.bold("\n2. Connecting to MongoDB:"))

    const DB = "enablerr_test"
    const COLLECTION = 'timesheets'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)

    // 3. Generate timesheets and WRITE them in batches to files
    console.log(chalk.cyan.bold("\n3. Generating time sheets:"))

    const numDocuments = 1000000 // total number of timesheet documents that you want to generate
    const minDates = 5	  // minimum amount of combinations
    const maxDates = 30    // maximum amount of combinations
    const batchSize = 1000000  // number of timesheets to store in each JSON file 

    var timesheet; 
    var timesheets = []; // timesheets as Javascript objects to import into Mongo

    var timesheet_export;
    var timesheets_export = []; // timesheets to save to file with proper formatting

    var count = 0
    var outputFiles = []

    function getRandomInt(min, max) {
        min = Math.ceil(min);
        max = Math.floor(max);
        return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
    }

    // do the insertions
    try {

        while ((count + timesheets.length) < numDocuments) { 
            var project = projects[getRandomInt(0, projects.length)]._id["$oid"]
            var employee = employees[getRandomInt(0, employees.length)]._id["$oid"]
            var service = services[getRandomInt(0, services.length)]._id["$oid"]

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

                // save one batch to file and db, end the process if enough time sheets
                if ((timesheets.length >= batchSize) || ((count + timesheets.length) >= numDocuments)){
                    count += timesheets.length

                    var outputPath = './data/output/timesheet_' + count + '.json'
                    fs.writeFileSync(outputPath, JSON.stringify(timesheets_export, null, 2))
                    outputFiles.push(outputPath)
                    timesheets_export = []
                    console.log(chalk.yellow.italic("* Saved batch to: " + outputPath))

                    // Timer
                    var start = process.hrtime();
                    console.log(chalk.bgRed.bold("\nTiming insertMany of " + numDocuments + " documents in MongoDB...")); // timer start

                    await collection.insertMany(timesheets)
                    timesheets = []

                    // stopping time and loging to console
                    var end = process.hrtime(start);
                    console.log(chalk.bgRed.bold(`\nExecution time: ${end[0]}s ${end[1] / 1000000}ms\n`));

                    console.log(chalk.yellow.italic("* Inserted batch to MongoDB: " + DB + '.' + COLLECTION))

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

run().catch(console.dir);
