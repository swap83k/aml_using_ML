# aml_using_ML
`curl -m 70 -X POST <https end point> /get_predictions \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-d '{
  "project_id": "project_id",
  "dataset_id": "project_id.ML_HACK",
  "model_id": "project_id.ML_HACK.aml_chk_model",
  "input_table_id": "training_view",
  "output_table_id": "traiining_op",
  "other_param": "other_value"
}`

 > Replace the project id and https end point
