var faker = require('faker');
require('dotenv').config()
var MongoClient = require('mongodb').MongoClient;

// Connect to the db
const mongodb_uri = process.env.DB_URI

MongoClient.connect(mongodb_uri, async function (err, client) {
    if (err) throw er

    var db = client.db('data_generation');

    const collection = db.collection('timesheets')

    const numProjects = 100
    const numEmployees = 50
    const numServices = 20
    const numDates = 10

    // "id": "aaa-bbb-aaa-005", -> MongoDB

    for (let i = 0; i < numProjects; i++) { 
        // "ref-project": "aaa-aaa-ddd-004",
        var project = faker.random.uuid()
        
        for (let j = 0; j < numEmployees; j++) { 
            // "ref-employee": "aaa-aaa-bbb-002",
            var employee = faker.random.uuid()
    
            for (let k = 0; k < numServices; k++) { 
                // "ref-service": "aaa-aaa-aaa-002",
                var service = faker.random.uuid()

                for (let l = 0; l < numDates; l++) { 
                    // "date": "2020-11-01",
                    // TODO: check not repeating
                    // TODO: limit period
                    // TODO: set it to date only
                    var date = faker.date.past()

                    // "quantity": 1.5
                    var quantity = faker.random.number() % 8 + (faker.random.number() % 10) / 10.0

                    await collection.insertOne({ 
                        "ref-project": project, 
                        "ref-employee": employee,
                        "ref-service": service, 
                        date,
                        quantity

                    });    

                }
            }
        }
    }
     
    
    collection.countDocuments(function (err3, count) {
        if (err3) throw err3;
        
        console.log('Total Rows: ' + count);
    });

    client.close();
                
});
