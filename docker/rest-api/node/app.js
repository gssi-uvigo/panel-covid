// Set up dependencies
const express = require('express')
const router = require('./routes/routes')
const { MongoClient } = require("mongodb");

const app = express();

// Web port
const port = 80;

// Database connection parameters
const username = "data_read"
const password = "givemesomedata"
const host = "database"
const auth_database = "covid_admin"
const dbname = "covid_analyzed_data"
const uri = `mongodb://${username}:${password}@${host}/${auth_database}`

// Connect to the database
MongoClient.connect(uri, function (err, client) {
    if (err) throw err;
    console.log("Connected to MongoDB")
    const database = client.db(dbname)

    // Set up routes
    router(app, database);

    // Initialize the server
    const server = app.listen(port, (error) => {
        if (error) return console.log(`Error: ${error}`);
        console.log(`Server listening on port ${server.address().port}`);

    });

});

