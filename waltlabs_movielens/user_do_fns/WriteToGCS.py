import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO

class WriteToGCS(beam.DoFn):
    def __init__(self, gcs_bucket, gcs_output_prefix):
        self.gcs_bucket = gcs_bucket
        self.gcs_output_prefix = gcs_output_prefix

    def process(self, element):
        filename, content = element
        gcs_path = f'gs://{self.gcs_bucket}/{self.gcs_output_prefix}/{filename}'
        with GcsIO().open(gcs_path, 'w') as gcs_file:
            gcs_file.write(content)