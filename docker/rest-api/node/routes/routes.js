const endpoints_list = ['/', '/cases', '/deaths', '/hospitalizations', '/hospitals_pressure', '/diagnostic_tests', '/covid_vs_all_deaths', '/outbreaks_description', '/top_death_causes', '/transmission_indicators', '/vaccination/general', '/vaccination/ages/single', '/vaccination/ages/complete', '/population_pyramid_variation']

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
     * GET /deaths
     * Return the daily deaths data.
    */
    app.get('/deaths', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['autonomous_region', 'age_range', 'gender', 'date'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database for the daily deaths
        const query = db.collection('deaths').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
        });

    /** 
     * GET /hospitalizations
     * Return the daily hospitalizations data.
    */
    app.get('/hospitalizations', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['autonomous_region', 'age_range', 'gender', 'date'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database for the daily hospitalizations
        const query = db.collection('hospitalizations').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
        });

    /** 
     * GET /diagnostic_tests
     * Return the daily diagnostic tests data.
    */
    app.get('/diagnostic_tests', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['autonomous_region', 'date'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database for the daily diagnostic tests
        const query = db.collection('diagnostic_tests').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
        });

    /** 
     * GET /outbreaks_description
     * Return the available outbreaks data.
    */
    app.get('/outbreaks_description', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['scope', 'date'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database for the outbreaks description data
        const query = db.collection('outbreaks_description').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });

    /** 
     * GET /top_death_causes
     * Return the top death causes in Spain.
    */
    app.get('/top_death_causes', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['gender', 'age_range', 'death_cause'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database for the top death causes data
        const query = db.collection('top_death_causes').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });
    
    /** 
     * GET /covid_vs_all_deaths
     * Return the percentage of deaths caused by COVID.
    */
    app.get('/covid_vs_all_deaths', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['gender', 'age_range'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database
        const query = db.collection('covid_vs_all_deaths').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });
    
    /** 
     * GET /transmission_indicators
     * Return the transmission indicators.
    */
    app.get('/transmission_indicators', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['date', 'autonomous_region'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database
        const query = db.collection('transmission_indicators').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });

    /** 
     * GET /symptoms
     * Return the most common symptoms.
    */
    app.get('/symptoms', (request, response) => {
        // Query the database
        db.collection('symptoms').find().toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });

    /** 
     * GET /vaccination/general
     * Return the general vaccination data.
    */
    app.get('/vaccination/general', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['date', 'autonomous_region'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database
        const query = db.collection('vaccination_general').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });

    /** 
     * GET /vaccination/ages/single
     * Return the single and complete vaccination data for each range and Autonomous Region.
    */
     app.get('/vaccination/ages/single', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['date', 'autonomous_region', 'age_range'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database
        const query = db.collection('vaccination_ages_single').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });

    /** 
     * GET /vaccination/ages/complete
     * Return the complete vaccination data for each age range and Autonomous Region.
    */
     app.get('/vaccination/ages/complete', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['date', 'autonomous_region', 'age_range'])

        // Get the projected columns, if any
        const projection = projectColumns(request)

        // Query the database
        const query = db.collection('vaccination_ages_complete').find(filters).project(projection)
        limitQuerySize(request, query).toArray(function (err, result) {
            if (err) response.sendStatus(500);
            response.send(result);
        });

        // Log the request in the console
        logRequest(request, response);
    });

    /** 
     * GET /population_pyramid_variation
     * Return the population pyramid variation.
    */
    app.get('/population_pyramid_variation', (request, response) => {
        // Get the request parameters, if any
        const filters = getQueryFilters(request, ['gender', 'age_range'])

        // Query the database
        const query = db.collection('vaccination').find(filters).toArray(function (err, result) {
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