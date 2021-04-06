var chalk = require('chalk');
const axios = require('axios');
var path = require('path');

require('dotenv').config()
const { ObjectID } = require('mongodb');

// Read input files
'use strict';
const fs = require('fs');

const generateRequest = (collection, id) => {
    var url = `${process.env.API_URL}/crud/${collection}`

    if (id) return url + `/${id}`
    else return url
}

async function wait(ms) {
    return new Promise(resolve => {
      setTimeout(resolve, ms);
    });
  }

exports.run = async () => {

    var fileList = [
        //path.join(__dirname, '..', '..', 'data', 'input', 'customer.json'),
        //path.join(__dirname, '..', '..', 'data', 'output', 'customer_500.json'),
        path.join(__dirname, '..', '..', 'data', 'output', 'customer_test_1000.json')//,
        //path.join(__dirname, '..', '..', 'data', 'output', 'customer.json')//,
        // path.join(__dirname, '..', '..', 'data', 'input', 'employee.json'),
        // path.join(__dirname, '..', '..', 'data', 'input', 'project-edited.json'),
        // path.join(__dirname, '..', '..', 'data', 'input', 'service.json'),
        // path.join(__dirname, '..', '..', 'data', 'output', 'timesheet_100.json'),
        // path.join(__dirname, '..', '..', 'data', 'output', 'timesheet_200.json'),
        // path.join(__dirname, '..', '..', 'data', 'output', 'timesheet_300.json')    
    ]

    for (var file of fileList) {
        //read the docs
        console.log(chalk.cyan.bold("\n * Reading file: " + file))
        var rawdata = fs.readFileSync(file);
        let documents = JSON.parse(rawdata);
        console.log("\t - Read " + documents.length + " documents");

        var inserted = []

        // insert them
        var collection = path.basename(file).split('.')[0].split('-')[0].split('_')[0]
        console.log("\t - Inserting at Collection '" + collection + "'");
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
                var response = await axios.post(url, document);     
                inserted.push(response.data) 

            } catch (error) {
                console.error(chalk.redBright.bold(error.message));
                return
            }           
        }
        
        await wait(2000)

        console.log("\t - Updating at Collection '" + collection + "'");
        for(var document of inserted) {
            try {
                // TODO for other types of documents
                var updated_document = { name: document.name + ' GmBH'}
                url = generateRequest(collection, document._id)
                var response = await axios.patch(url, updated_document);     
            } catch (error) {
                console.error(chalk.redBright.bold(error.message));
                return
            }           
        }

        await wait(2000)

        console.log("\t - Querying current version at Collection '" + collection + "'");
        for(var document of inserted) {
            try {
                url = generateRequest(collection, document._id)
                var response = await axios.get(url);     
            } catch (error) {
                console.error(chalk.redBright.bold(error.message));
                return
            }           
        }
        await wait(2000)

        console.log("\t - Querying past version at Collection '" + collection + "'");
        for(var document of inserted) {
            try {
                url = generateRequest(collection, document._id)
                var response = await axios.get(url + "/1");     
            } catch (error) {
                console.error(chalk.redBright.bold(error.message));
                return
            }           
        }

        await wait(2000)

        console.log("\t - Deleting from Collection '" + collection + "'");
        for(var document of inserted) {
            try {
                url = generateRequest(collection, document._id)
                var response = await axios.delete(url);     
            } catch (error) {
                console.error(chalk.redBright.bold(error.message));
                return
            }           
        }

    } 
}
