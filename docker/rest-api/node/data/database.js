// Set up the connection with the MongoDB database
const { MongoClient } = require("mongodb");

// Database connection parameters
const username = "data_read"
const password = "givemesomedata"
const host = "airflow-database"
const auth_database = "covid_admin"
const database = "covid_analyzed_data"
const uri = `mongodb://${username}:${password}@${host}/${auth_database}`

// Connect to the database
const mongoClient = new MongoClient(uri);

module.exports = mongoClient;