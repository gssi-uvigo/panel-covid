// User that allows Airflow to create the collections and fill them with data for the extracted and analyzed data databases
db.createUser(
    {
        user: "airflow_user",
        pwd: "contrasenha123",
        roles: [
            {
                role: "dbOwner",
                db: "covid_extracted_data"
            },
            {
                role: "dbOwner",
                db: "covid_analyzed_data"
            }
        ]
    }
);

// User for reading the analyzed data, in order to visualize it or make it available through an API
db.createUser(
    {
        user: "data_read",
        pwd: "givemesomedata",
        roles: [
            {
                role: "read",
                db: "covid_analyzed_data"
            }
        ]
    }
);