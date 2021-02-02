const mongoClient = require('../data/database')

const endpoints_list = ['/', '/cases']

/**
 * Log a request datetime, client IP, method, endpoint and response status code in the console.
 * @param {http.ClientRequest} request Client HTTP request
 * @param {http.ServerResponse} response HTTP response to the client
 */
function logRequest(request, response) {
    console.log(`[${new Date().toISOString()}] ${request.connection.remoteAddress}: ${response.statusCode} ${request.method} ${request.path}`);
}

const router = app => {

    /** 
     * GET /
     * Return all the available endpoints
    */
    app.get('/', (request, response) => {
        // List all the available endpoints
        response.send(endpoints_list);

        // Log the request in the console
        logRequest(request, response);
    });

    /**
     * Invalid endpoint
     */
    app.all('/*', (request, response) => {
        // Return a 404
        response.sendStatus(404);

        // Log the request in the console
        logRequest(request, response);
    })

}

module.exports = router;