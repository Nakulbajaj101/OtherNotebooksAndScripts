from google.cloud import bigquery
import logging

###creating a dataset###

def create_dataset(project = None, location = "EU", dataset_id = "", confirm_dataset = "y"):
    #This will either create the new dataset or use based on users input
    client = bigquery.Client(project=project)

    confirm_dataset = confirm_dataset
    if confirm_dataset == 'n':

        dataset_id = dataset_id
        logging.info("The dataset that will be created:  {}".format(dataset_id))
        dataset_ref = client.dataset(dataset_id)

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_ref)

        # Specify the geographic location where the dataset should reside.
        dataset.location = location
        try:
            dataset = client.create_dataset(dataset)
        except Exception as e:
            logging.info(e)
    else:
        dataset_id = dataset_id
        logging.info("dataset that will be used is : {}".format(dataset_id))
    return dataset_id


###creating a big query table###
def create_bq_table(project = None, dataset = None, location = "EU", file_location = None, table = None, feature_table = "", integration_table = ""):
    #The idea is based on your query create a table with all the raw transactions, please make sure that date_date field is aliased as
    #transaction_date

    sql_file_location = file_location
    if table == None:
        table = input("Please input the name of the table") + ".sql"
    elif table[-1] == '_':
        table = table[:-1] + ".sql"
    else:
        table = table + ".sql"
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    dataset_id = dataset

    # Set the destination table

    table_id = table.split(".sql")[0]
    logging.info("table name is {}".format(table_id))
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref


    with open(sql_file_location + "/" +table, 'r') as myfile:
        query=myfile.read()
        query=query.replace('feature_dataset_id',feature_table)
        query=query.replace('integration_dataset_id',integration_table)


    # Start the query, passing in the extra configuration.
    #checking if the table already exists, if it does, try block will catch it
    try:
        query_job = client.query(
            query,
            job_config=job_config)
        query_job.result() # API request - starts the query
    except Exception as e:
        logging.info(e)
        logging.info("failed checking if table already exists")
        job_config.create_disposition = "CREATE_NEVER"
        job_config.write_disposition = "WRITE_TRUNCATE"
        query_job = client.query(
            query,
            job_config=job_config)
        query_job.result()
    # API request - starts the query
    # Waits for the query to finish

    logging.info('Query results loaded to table {}'.format(table_ref.path))
    return table_id
