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
    console.log(chalk.cyan.bold("\n1. Connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const COLLECTION = 'services'

    var MongoClient = require('mongodb').MongoClient;
    const mongodb_uri = process.env.DB_URI
    const client = new MongoClient(mongodb_uri, { useUnifiedTopology: true } );

    await client.connect();
    console.log(chalk.yellow.italic(" * Connected to DB"))
    const db = client.db(DB);
    const collection = db.collection(COLLECTION)

    // 2. Generate employees and WRITE them in batches to files
    console.log(chalk.cyan.bold("\n2. Generating documents:"))

    // TODO: Guess all this params from numDocuments (original 35000)
    const batchSize = (numDocuments>500) ? Math.max(1, numDocuments/10) : numDocuments // number of documents to store in each JSON file 

    var document; 
    var documents = []; // documents as Javascript objects to import into Mongo

    var document_export;
    var documents_export = []; // documents to save to file with proper formatting

    var count = 0
    var outputFiles = []

    // do the insertions
    try {

        while ((count + documents.length) < numDocuments) {   
          
            // added for customers
            var servno = 100000 + count + documents.length
            
            var descr = faker.name.jobArea() + ' ' + faker.name.jobType() + ' ' + faker.name.jobType() + '$';
            if (descr.endsWith('or$')) descr = descr.replace('or$', 'ion')
            else descr = descr.replace('$', ' Services')
            
            var type = "Stunde";
            var price = getRandomInt(50, 500)
            
            document = { 
                servno: servno,
                descr: descr,
                type: type,
                price: price
            }    

            document_export = { 
                servno: servno,
                descr: descr,
                type: type,
                price: price
            }    

            documents.push(document)
            documents_export.push(document_export)

            // save one batch to file and db, end the process if enough time sheets
            if ((documents.length >= batchSize) || ((count + documents.length) >= numDocuments)){
                count += documents.length

                var outputPath = path.join(__dirname, '..', 'data', 'output', 'service_' + count + '.json');
                fs.writeFileSync(outputPath, JSON.stringify(documents_export, null, 2))
                outputFiles.push(outputPath)
                documents_export = []
                console.log(chalk.yellow.italic("* Saved batch to: " + outputPath))

                console.log(chalk.yellow.italic("* Inserting batch to MongoDB: " + collection.namespace))

                // Timer
                var start = process.hrtime();
                console.log(chalk.magenta("\t - Timing insertMany of " + documents.length + " documents in MongoDB...")); // timer start

                await collection.insertMany(documents)
                documents = []

                // stopping time and loging to console
                var end = process.hrtime(start);
                console.log(chalk.magenta.bold(`\t\t ... execution time: ${end[0]}s ${end[1] / 1000000}ms\n`));


                //exit loop if enough total documents
                if (count >= numDocuments) break
                else console.log("- Generated \t" + (documents.length + count) + "/" + numDocuments)
            }
        }

    } finally {
        client.close();                
    }

    console.log(chalk.green.bold("\n Insertion script for '" + COLLECTION + "' finished successfully!!\n"))
}
