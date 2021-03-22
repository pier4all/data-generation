var chalk = require('chalk');
const axios = require('axios');

require('dotenv').config()
const { ObjectID } = require('mongodb');


// Read input files
'use strict';
const fs = require('fs');

const generateRequest = (collection) => {
    var url = `${process.env.API_URL}/${collection}/create`
    return url
}

async function run() {

    var fileList = [
        './data/input/customer.json',
        './data/input/employee.json',
        './data/input/project-edited.json',
        './data/input/service.json'
    ]

    for (var file of fileList) {
        //read the docs
        console.log(chalk.cyan.bold("\n * Reading file: " + file))
        var rawdata = fs.readFileSync(file);
        let documents = JSON.parse(rawdata);
        console.log("\t - Read " + documents.length + " documents");

        // insert them
        var collection = file.split('/')[3].split('.')[0].split('-')[0]
        console.log("\t - Collection '" + collection + "'");
        var url = generateRequest(collection)
        for(var document of documents) {
            try {
                document._id =  document._id["$oid"]
                var response = await axios.post( url, document);            
            } catch (error) {
                console.error(error.message);
                break
            }           
        }
    } 
}

run().catch(console.dir);
