'use strict';

var faker = require('faker');
var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
const fs = require('fs');
const { random } = require('faker');
var path = require('path');

exports.run = async (numDocuments = 20) => {

     // 1. READ input files
     console.log(chalk.cyan.bold("\n1. Reading input files:"))

     var customerPath = path.join(__dirname, '..', 'data', 'input', 'customer.json');
     var rawdata = fs.readFileSync(customerPath);
     let customers = JSON.parse(rawdata);
     console.log(" - Read " + customers.length + " customers");

     var employeePath = path.join(__dirname, '..', 'data', 'input', 'employee.json');
     rawdata = fs.readFileSync(employeePath);
     let employees = JSON.parse(rawdata);
     console.log(" - Read " + employees.length + " employees");

    // 2. Connect to the db
    console.log(chalk.cyan.bold("\n2. Connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const COLLECTION = 'projects'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)

    // 3. Generate documents and WRITE them in batches to files
    console.log(chalk.cyan.bold("\n3. Generating documents:")) 

    // TODO: Guess all this params from numDocuments (original 50000)
    const minDates = Math.min(2, numDocuments)	  // minimum amount of documents per input combination
    const maxDates = Math.min(10, numDocuments)    // minimum amount of documents per input combination
    const batchSize = (numDocuments>500) ? Math.max(1, numDocuments/10) : numDocuments // number of documents to store in each JSON file 

    var document; 
    var documents = []; // documents as Javascript objects to import into Mongo

    var document_export;
    var documents_export = []; // documents to save to file with proper formatting

    var count = 0
    var outputFiles = []

    /// NECESSARY?
    function getRandomInt(min, max) {
        min = Math.ceil(min);
        max = Math.floor(max);
        return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
    }

    // do the insertions
    try {

        while ((count + documents.length) < numDocuments) { 
            var customer = customers[getRandomInt(0, customers.length)]._id["$oid"]
            var employee = employees[getRandomInt(0, employees.length)]._id["$oid"]

            /// NECESSARY?
            var numDates = getRandomInt(minDates, maxDates)
            for (let l = 0; l < numDates; l++) {              

                // added for projects
                var title = faker.commerce.productName();
                              
                                
                document = { 
                    title: title,
                    "ref-customer": new ObjectID(customer),
                    "ref-employee": new ObjectID(employee)
                }    

                document_export = { 
                    title: title,
                    "ref-customer": { "$oid": new ObjectID(customer) },
                    "ref-employee": { "$oid": new ObjectID(employee) }
                }    

                documents.push(document)
                documents_export.push(document_export)

                // save one batch to file and db, end the process if enough time sheets
                if ((documents.length >= batchSize) || ((count + documents.length) >= numDocuments)){
                    count += documents.length

                    var outputPath = path.join(__dirname, '..', 'data', 'output', 'project_' + count + '.json');
                    fs.writeFileSync(outputPath, JSON.stringify(documents_export, null, 2))
                    outputFiles.push(outputPath)
                    documents_export = []
                    console.log(chalk.yellow.italic("* Saved batch to: " + outputPath))

                    // Timer
                    var start = process.hrtime();
                    console.log(chalk.bgRed.bold("\nTiming insertMany of " + numDocuments + " documents in MongoDB...")); // timer start

                    await collection.insertMany(documents)
                    documents = []

                    // stopping time and loging to console
                    var end = process.hrtime(start);
                    console.log(chalk.bgRed.bold(`\nExecution time: ${end[0]}s ${end[1] / 1000000}ms\n`));

                    console.log(chalk.yellow.italic("* Inserted batch to MongoDB: " + DB + '.' + COLLECTION))

                    //exit loop if enough total documents
                    if (count >= numDocuments) break
                }
            }
            if (count < numDocuments) console.log("- Generated \t" + (documents.length + count) + "/" + numDocuments)
        }

    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\n Insertion script finished successfully!!\n"))
}
