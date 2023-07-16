create or replace view `aml_schema.aml_training_view`
as select customer_id,country_of_residence,country_of_birth,trasnaction_amount,transaction_country,
transaction_category,transaction_country_category,transaction_origin_currency,transaction_date,
transaction_type,transaction_business,average_monthly_transaction,alert_flag
from `<project_name>.aml_schema.aml_input_train`

 CREATE OR REPLACE MODEL
				  `aml_schema.predict_transactions`
				OPTIONS
				  ( model_type='LOGISTIC_REG',
          WARM_START=TRUE,
					auto_class_weights=TRUE,
					input_label_cols=['alert_flag']
				  ) AS
				SELECT
				  * 
				FROM
				  `aml_schema.aml_training_view`
				  
#standardSQL
SELECT
  *
FROM
  ML.EVALUATE(MODEL `aml_schema.predict_transactions`, (
SELECT
  *
FROM
  `aml_schema.aml_input_eval`
));


SELECT
  *
FROM
  ML.PREDICT (MODEL `aml_schema.predict_transactions`,
    (
    SELECT
      *
    FROM
      `aml_schema.aml_input_predict`
    )
  );
  
  
select * from ml.training_info(MODEL `aml_schema.predict_transactions`);
