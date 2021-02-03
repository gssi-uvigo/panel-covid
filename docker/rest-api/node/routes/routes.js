const { query } = require("express");

const endpoints_list = ['/', '/cases']

/**
 * Log a request datetime, client IP, method, endpoint and response status code in the console.
 * @param {http.ClientRequest} request Client HTTP request
 * @param {http.ServerResponse} response HTTP response to the client
 */
function logRequest(request, response) {
    console.log(`[${new Date().toISOString()}] ${request.ip}: ${response.statusCode} ${request.method} ${request.originalUrl}`);
}

/**
 * Return a dictionary with the filters to pass to the MongoDB query
 * @param {http.ClientRequest} request Client HTTP request
 * @param {object} filterNames Array with the names of the available filters 
 * @returns {object} Dictionary with the filters
 */
function getQueryFilters(request, filterNames) {
    var filters = {}
    filterNames.forEach((name) => {
        // Check if this filter has been specified by the client
        var queryFilterValue = request.query[name]
        if(queryFilterValue) {
            // The client applied this filter
            if(name == 'date') {
                // Parse the date
                queryFilterValue = new Date(queryFilterValue)
            }

            // Add the filter to the list
            filters[name] = queryFilterValue
        }
    })

    return filters
}

/**
 * Return a dictionary with the projection to pass to the MongoDB query
 * @param {http.ClientRequest} request Client HTTP request
 * @returns {object} Dictionary with the columns projection
 */
function projectColumns(request) {
    const response_contents = request.query['response_contents']
    var projection = {'_id': 0}
    if(response_contents) {
        if(typeof(response_contents)=='string') {
            projection[response_contents] = 1
        }else{
            response_contents.forEach((value) => {projection[value] = 1})
        }

    }

    return projection
}

/**
 * Limit the query size, if the user requested so
 * @param {http.ClientRequest} request Client HTTP request
 * @param {} mongoQuery MongoDB query object
 * @returns {} Paginated MongoDB query
 */
function limitQuerySize(request, mongoQuery) {
    // Get the pagination configuration, if any
    const limit = parseInt(request.query.limit) || 0
    const page = parseInt(request.query.page) || 0

    return mongoQuery.skip(page*limit).limit(limit)

}


const router = (app, db) => {

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
     * GET /cases
     * Return the daily cases data.
    */
    app.get('/cases', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['autonomous_region', 'age_range', 'gender', 'date'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database for the daily cases
        const query = db.collection('cases').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

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