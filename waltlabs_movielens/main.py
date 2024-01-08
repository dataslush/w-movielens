import os
import apache_beam as beam
import logging
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from waltlabs_movielens.user_do_fns import (
    ExtractZip,
    WriteToGCS,
    DownloadFromUrl,
    ReadParseRatingsCSV,
    ReadParseMoviesCSV
)


MOVIELENS_DATASET = "ml-20m"
ZIP_FILE_URL = f"https://files.grouplens.org/datasets/movielens/{MOVIELENS_DATASET}.zip"

# BigQuery table schemas
MOVIES_SCHEMA = {
    'fields': [
        {'name': 'movieId', 'type': 'INTEGER'},
        {'name': 'title', 'type': 'STRING'},
        {'name': 'genres', 'type': 'STRING', 'mode': 'REPEATED'}
    ]
}

RATINGS_SCHEMA = {
    'fields': [
        {'name': 'userId', 'type': 'INTEGER'},
        {'name': 'movieId', 'type': 'INTEGER'},
        {'name': 'rating', 'type': 'FLOAT'},
        {'name': 'timestamp', 'type': 'INTEGER'}
    ]
}

class MovielensOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--gcs_bucket", help="Google Cloud Storage bucket name")

        parser.add_argument(
            "--gcs_output_prefix",
            help="GCS output prefix for CSV files",
            default=os.environ.get("GCS_OUTPUT_PREFIX", "tables"),
        )

        parser.add_argument(
            "--bq_dataset",
            help="BigQuery Dataset for MovieLens Data"
        )

        parser.add_argument(
            "--gcp_project",
            help="GCP Project Name"
        )


options = MovielensOptions()
pipeline = beam.Pipeline(options=options)


zip_file_download = (
    pipeline
    | "Create" >> beam.Create([ZIP_FILE_URL])
    | "Download Zip File" >> beam.ParDo(DownloadFromUrl())
)

_ = zip_file_download | "Write raw zip file to GCS" >> beam.io.WriteToText(
    file_path_prefix=f"gs://{options.gcs_bucket}/raw/{MOVIELENS_DATASET}.zip",
    num_shards=1,
    shard_name_template="",
)

files_to_gcs = (
    zip_file_download
    | "Extract Files" >> beam.ParDo(ExtractZip())
    | "Write Files to GCS"
    >> beam.ParDo(WriteToGCS(options.gcs_bucket, options.gcs_output_prefix))
)

_ = (
    files_to_gcs
    | 'Read & Parse Movies CSV' >> beam.ParDo(ReadParseMoviesCSV())
    | 'Write Movies to BigQuery' >> WriteToBigQuery(
        table='movies',
        dataset=options.bq_dataset,
        project=options.gcp_project,
        schema=MOVIES_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS
    )
)

# _ = (
#     files_to_gcs
#     | 'Read & Parse Ratings CSV' >> beam.ParDo(ReadParseRatingsCSV())
#     | 'Write Ratings to BigQuery' >> WriteToBigQuery(
#         table='ratings',
#         dataset=options.bq_dataset,
#         project=options.gcp_project,
#         schema=RATINGS_SCHEMA,
#         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#         method=beam.io.WriteToBigQuery.Method.FILE_LOADS
#     )
# )

result = pipeline.run()
result.wait_until_finish()
