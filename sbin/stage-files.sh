python dataflow-pipelines/movielens_to_bq.py \
    --gcs_bucket=fk_mkt \
    --runner=DataflowRunner \
    --region=us-central1 \
    --project=tenacious-camp-357012 \
    --temp_location=gs://fk_mkt/tmp/ \
    --staging_location=gs://fk_mkt/staging/
