'use strict';

var faker = require('faker');
var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
const fs = require('fs');
const { random } = require('faker');


exports.run = async () => {


    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Connecting to MongoDB:"))

    const DB = "enablerr_test"
    const COLLECTION = 'customers'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)

    // 2. Generate employees and WRITE them in batches to files
    console.log(chalk.cyan.bold("\n2. Generating documents:"))

    const numDocuments = 35000 // total number of documents that you want to generate
    const minDates = 10	  // minimum amount of documents per input combination
    const maxDates = 200    // minimum amount of documents per input combination
    const batchSize = 35000  // number of documents to store in each JSON file 

    var document; 
    var documents = []; // documents as Javascript objects to import into Mongo

    var document_export;
    var documents_export = []; // documents to save to file with proper formatting

    var count = 0
    var outputFiles = []

   
    function getRandomInt(min, max) {
        min = Math.ceil(min);
        max = Math.floor(max);
        return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
    }

    // do the insertions
    try {

        while ((count + documents.length) < numDocuments) { 

            
            var numDates = getRandomInt(minDates, maxDates)
            for (let l = 0; l < numDates; l++) {              

                // added for customers
                var name = faker.company.companyName();
                var email = faker.internet.email();
              
                const language = ["DE", "EN", "FR"];
                var random_language = language[Math.floor(Math.random() * language.length)];

                
                document = { 
                    name: name,
                    email: email,
                    "language": random_language
                }    

                document_export = { 
                    name: name,
                    email: email,
                    "language": random_language
                }    

                documents.push(document)
                documents_export.push(document_export)

                // save one batch to file and db, end the process if enough time sheets
                if ((documents.length >= batchSize) || ((count + documents.length) >= numDocuments)){
                    count += documents.length

                    var outputPath = './data/output/customer_' + count + '.json'
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

run().catch(console.dir);
