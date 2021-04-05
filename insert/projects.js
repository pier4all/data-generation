'use strict';

var faker = require('faker');
var chalk = require('chalk');
require('dotenv').config()
const {ObjectID} = require('mongodb');
const fs = require('fs');
const { random } = require('faker');
var path = require('path');

exports.run = async (numDocuments = 20) => {

    // 1. Connect to the db
    console.log(chalk.cyan.bold("\n1. Connecting to MongoDB:"))

    const DB = process.env.DB_NAME
    const COLLECTION = 'projects'

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
    console.log(" - Read " + customers.length + " customers");

    refcollection = db.collection("employees")
    console.log(" - Running query on " + refcollection.namespace + ", projection: " + JSON.stringify(projection));
    var employees = await refcollection.find({}, { projection } ).toArray()
    console.log(" - Read " + employees.length + " employees");

    // 3. Generate documents and WRITE them in batches to files
    console.log(chalk.cyan.bold("\n3. Generating documents:")) 

    // TODO: Guess all this params from numDocuments (original 50000)
    const batchSize = (numDocuments>500) ? Math.max(1, numDocuments/10) : numDocuments // number of documents to store in each JSON file 

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
            var customer = customers[getRandomInt(0, customers.length)]._id
            var employee = employees[getRandomInt(0, employees.length)]._id      

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

                console.log(chalk.yellow.italic("* Inserting batch to MongoDB: " + DB + '.' + COLLECTION))

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
