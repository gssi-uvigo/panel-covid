// Set up dependencies
const express = require('express')
const router = require('./routes/routes')

const app = express();

// Web port
const port = 80;

// Set up routes
router(app);

// Initialize the server
const server = app.listen(port, (error) => {
    if (error) return console.log(`Error: ${error}`);
    console.log(`Server listening on port ${server.address().port}`);

});