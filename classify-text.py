from google.cloud import storage, language_v1, bigquery

# Create a client for each GCP service that will be used
storage_client = storage.Client()
nl_client = language_v1.LanguageServiceClient()
bq_client = bigquery.Client(project='qwiklabs-gcp-00-d6e369ba5d0d')

# To get datasets and tables from BQ, first get the reference from the client
# then pass that reference as Dataset or as Table.
dataset_ref = bq_client.dataset('news_classification_dataset')
dataset = bigquery.Dataset(dataset_ref)
table_ref = dataset.table('article_data')
table = bq_client.get_table(table_ref)

# Just pass a text to the nl_client to classify it
def classify_text(article):
    response = nl_client.classify_text(
        document=language_v1.types.Document(
            content=article,
            type_='PLAIN_TEXT'
        )
    )
    return response

rows_for_bq = []

# Get all file names inside the bucket
files = storage_client.bucket('cloud-training-demos-text').list_blobs()
print("Got article files from GCS, sending them to the NL API (this will take 2 minutes)...")

for file in files:
    if file.name.endswith('txt'):
        article_text = file.download_as_bytes()
        nl_response = classify_text(article_text)
        if len(nl_response.categories) > 0:
            rows_for_bq.append((
                str(article_text),
                # More than 1 category can be assigned to each text, here we're keeping only the first one
                str(nl_response.categories[0].name),
                str(nl_response.categories[0].confidence),
            ))

print("Writing NL API article data to BigQuery...")

errors = bq_client.insert_rows(table, rows_for_bq)
assert errors == []
