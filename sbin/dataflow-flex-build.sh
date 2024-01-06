gcloud dataflow flex-template build ${GCS_PATH}/templates/${TEMPLATE_TAG}/${TEMPLATE_NAME}.json \
    --image ${TEMPLATE_IMAGE} \
    --sdk-language PYTHON \
    --metadata-file ${TEMPLATE_NAME}-metadata