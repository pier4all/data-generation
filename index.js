var faker = require('faker');
var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');

// Read input files
'use strict';
const fs = require('fs');

async function run() {
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


    // 2. Generate timesheets and WRITE them in batches to files
    console.log(chalk.cyan.bold("\n2. Generating time sheets:"))

    const numTimesheets = 300 // total number of timesheet documents that you want to generate
    const minDates = 10	  // minimum amount of timesheets per project-employee-service combination
    const maxDates = 80    // minimum amount of timesheets per project-employee-service combination
    const batchSize = 100  // number of timesheets to store in each JSON file 

    var timesheet;
    var timesheets = [];
    var count = 0
    var outputFiles = []

    function getRandomInt(min, max) {
        min = Math.ceil(min);
        max = Math.floor(max);
        return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
    }

    while ((count + timesheets.length) < numTimesheets) { 
        var project = projects[getRandomInt(0, projects.length)]._id["$oid"]
        var employee = employees[getRandomInt(0, employees.length)]._id["$oid"]
        var service = services[getRandomInt(0, services.length)]._id["$oid"]

        var numDates = getRandomInt(minDates, maxDates)
        for (let l = 0; l < numDates; l++) { 
            // TODO: check not repeating
            // TODO: limit period

            // toggle comment on next line to set it to date with zeroes or string
            //var date = new Date(faker.date.past().toISOString().slice(0,10));
            var date = faker.date.past().toISOString().slice(0,10);

            // "quantity": 1.5
            var quantity = (1 + faker.random.number()) % 8 + (faker.random.number() % 10) / 10.0

            timesheet = { 
                "ref-project": new ObjectID(project), 
                "ref-employee": new ObjectID(employee),
                "ref-service": new ObjectID(service), 
                date,
                quantity
            }    
            timesheets.push(timesheet)

            // save one batch to file and end the process
            if ((timesheets.length >= batchSize) || ((count + timesheets.length) >= numTimesheets)){
                count += timesheets.length
                var outputPath = './data/output/timesheet_' + count + '.json'
                fs.writeFileSync(outputPath, JSON.stringify(timesheets, null, 2))
                outputFiles.push(outputPath)
                timesheets = []
                console.log(chalk.yellow.italic("* Saved batch to: " + outputPath))
                if (count >= numTimesheets) break
            }
        }
        
        if (count < numTimesheets) console.log("- Generated \t" + (timesheets.length + count) + "/" + numTimesheets)
    }

    // 3. Read files and INSERT documents in the DB
    console.log(chalk.cyan.bold("\n3. Insert in MongoDB:"))

    // Connect to the db
    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    // do the insertions
    try {
        await client.connect();
        const db = client.db("data_generation");
        const collection = db.collection('timesheets')

        for (const outputPath of outputFiles) {
            console.log("- Reading file: " + outputPath)
            var rawdataOutput = fs.readFileSync(outputPath);
            var timesheetBatch = JSON.parse(rawdataOutput);
            await collection.insertMany(timesheetBatch);    

            var count = await collection.countDocuments()
            console.log(chalk.yellow('* Inserted rows: ' + count+ "/" + numTimesheets));
        };

    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\nDONE, script finished successfully!!\n"))
}

run().catch(console.dir);