db.createUser(
    {
        user: "airflow_user",
        pwd: "contrasenha123",
        roles: [
            {
                role: "readWrite",
                db: "covid_extracted_data"
            }
        ]
    }
);