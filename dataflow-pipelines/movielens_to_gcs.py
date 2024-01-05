import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystem import CompressionTypes

MOVIELENS_DATASET = 'ml-20m'
ZIP_FILE_URL = 'https://files.grouplens.org/datasets/movielens/ml-20m.zip'



class MovielensOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--gcs_bucket',
            help='Google Cloud Storage bucket name'
        )

        parser.add_argument(
            '--gcs_output_prefix',
            help='GCS output prefix for CSV files',
            default=os.environ.get('GCS_OUTPUT_PREFIX', 'tables')
        )

options = MovielensOptions()
pipeline = beam.Pipeline(options=options)

# Custom Transform to download files from a URL
class DownloadFromUrl(beam.DoFn):
    def process(self, element):
        import requests
        response = requests.get(element)
        yield response.content

# Custom Transform to extract CSV files from the zip archive
class ExtractZip(beam.DoFn):
    def process(self, element):
        from io import BytesIO
        import zipfile

        zip_data = BytesIO(element)
        with zipfile.ZipFile(zip_data) as zip_file:
            for file_info in zip_file.infolist():
                with zip_file.open(file_info) as file:
                    if file_info.filename.endswith('.csv'):
                        yield file_info.filename, file.read()

class WriteToGCS(beam.DoFn):
    def __init__(self, gcs_bucket, gcs_output_prefix):
        self.gcs_bucket = gcs_bucket
        self.gcs_output_prefix = gcs_output_prefix

    def process(self, element):
        from apache_beam.io.gcp.gcsio import GcsIO
        filename, content = element
        gcs_path = f'gs://{self.gcs_bucket}/{self.gcs_output_prefix}/{filename}'
        with GcsIO().open(gcs_path, 'w') as gcs_file:
            gcs_file.write(content)


zip_file_download = (
    pipeline
    | 'Create' >> beam.Create([ZIP_FILE_URL]) 
    | 'Download Zip File' >> beam.ParDo(DownloadFromUrl())
)

_ = (
    zip_file_download
    | 'Write raw zip file to GCS' >> beam.io.WriteToText(
        file_path_prefix=f'gs://{options.gcs_bucket}/raw/{MOVIELENS_DATASET}.zip',
        num_shards=1,
        shard_name_template=''
    )
)

_ = (
    zip_file_download
    | 'Extract Files' >> beam.ParDo(ExtractZip())
    | 'Write Files to GCS' >> beam.ParDo(WriteToGCS(options.gcs_bucket, options.gcs_output_prefix))
)
result = pipeline.run()
result.wait_until_finish()
