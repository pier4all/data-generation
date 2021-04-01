var chalk = require('chalk');
const axios = require('axios');
var path = require('path');

require('dotenv').config()
const { ObjectID } = require('mongodb');

// Read input files
'use strict';
const fs = require('fs');

const generateRequest = (collection) => {
    var url = `${process.env.API_URL}/crud/${collection}`
    return url
}

exports.run = async () => {

    var fileList = [
        path.join(__dirname, '..', '..', 'data', 'input', 'customer.json'),
        path.join(__dirname, '..', '..', 'data', 'input', 'employee.json'),
        path.join(__dirname, '..', '..', 'data', 'input', 'project-edited.json'),
        path.join(__dirname, '..', '..', 'data', 'input', 'service.json'),
        path.join(__dirname, '..', '..', 'data', 'output', 'timesheet_100.json'),
        path.join(__dirname, '..', '..', 'data', 'output', 'timesheet_200.json'),
        path.join(__dirname, '..', '..', 'data', 'output', 'timesheet_300.json')    
    ]

    for (var file of fileList) {
        //read the docs
        console.log(chalk.cyan.bold("\n * Reading file: " + file))
        var rawdata = fs.readFileSync(file);
        let documents = JSON.parse(rawdata);
        console.log("\t - Read " + documents.length + " documents");

        // insert them
        var collection = path.basename(file).split('.')[0].split('-')[0].split('_')[0]
        console.log("\t - Collection '" + collection + "'");
        var url = generateRequest(collection)
        for(var document of documents) {
            try {
                Object.keys(document).forEach(function(key) {
                    var value = document[key]
                    var tags = ["$oid", "$date"]
                    tags.forEach( tag => {
                        if ((!!value) && (value.constructor === Object) 
                                      && (value.hasOwnProperty(tag))) {
                            document[key] = value[tag]
                        }
                    })
                }); 
                var response = await axios.post( url, document);            
            } catch (error) {
                console.error(chalk.redBright.bold(error.message));
                return
            }           
        }
    } 
}
