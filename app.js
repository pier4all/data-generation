const chalk = require('chalk')
const yargs = require('yargs')

//Custoize your yargs version
yargs.version('1.1.0')

// Create insert command
yargs.command({
    command: 'insert',
    describe: 'Generate data',
    builder: {
        collection: {
            describe: 'Collection',
            demandOption: true,
            type: 'string'
        },
        numDocs: {
            describe: 'Number of documents to generate',
            demandOption: true,
            type: 'number'
        }
    },
    handler: function(argv) {
        const generator = require('./insert/'+ argv.collection)
        generator.run(argv.numDocs)
    }
})

// Create insert custom command
yargs.command({
    command: 'insert-custom',
    describe: 'Run custom insert script',
    builder: {
        script: {
            describe: 'Script name without extension',
            demandOption: true,
            type: 'string'
        },
        numDocs: {
            describe: 'Number of documents to generate',
            demandOption: false,
            type: 'number'
        }
    },
    handler: function(argv) {
        const generator = require('./insert/custom/'+ argv.script)
        generator.run(argv.numDocs)
    }
})

// Create insert custom command
yargs.command({
    command: 'query',
    describe: 'Run query script',
    builder: {
        script: {
            describe: 'Script name without extension',
            demandOption: true,
            type: 'string'
        }
    },
    handler: function(argv) {
        const generator = require('./query/'+ argv.script)
        generator.run()
    }
})

yargs.parse()