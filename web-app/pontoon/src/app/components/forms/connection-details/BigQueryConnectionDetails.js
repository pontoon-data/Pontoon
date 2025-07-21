import { Typography, Stack } from "@mui/material";
import * as Yup from "yup";

import FormTextInput from "@/app/components/forms/FormTextInput";

export const BigQueryConnectionDetails = ({ isDestination }) => {
  return (
    <>
      {!isDestination ? (
        <Stack>
          <Typography sx={{ fontWeight: 600, marginBottom: "4px" }}>
            Enter the source credentials
          </Typography>
          <Typography>
            Provide the details to connect to this BigQuery source.
          </Typography>
        </Stack>
      ) : null}
      <FormTextInput
        label="Project ID"
        name="bigquery.project_id"
        placeholder="my-project-12345"
      />
      {isDestination === true ? (
        <>
          <FormTextInput
            label="GCS Bucket"
            name="bigquery.gcs_bucket_name"
            placeholder="gs://mybucket"
          />

          <FormTextInput
            label="GCS Bucket Prefix"
            name="bigquery.gcs_bucket_path"
            placeholder="/exports"
          />

          <FormTextInput
            label="Destination Schema"
            name="bigquery.target_schema"
            placeholder="export"
          />
        </>
      ) : null}
      <FormTextInput
        label="Service Account Key"
        name="bigquery.service_account"
        type="password"
        autoComplete="current-password"
        placeholder="************"
        helperText="The Service Account Key should be provided as a single JSON object from GCP, without modification."
      />
    </>
  );
};

export const getBigQueryValidation = (isDestination) => {
  let schema = Yup.object().shape({
    project_id: Yup.string().required("Required"),
    service_account: Yup.string().required("Required"),
  });

  if (isDestination) {
    const destinationSchema = schema.shape({
      target_schema: Yup.string().required("Required"),
      gcs_bucket_name: Yup.string().required("Required"),
      gcs_bucket_path: Yup.string().required("Required"),
    });
    schema = schema.concat(destinationSchema);
  }
  return schema;
};

export const getBigQueryInitialValues = (isDestination) => {
  const initialVals = {
    project_id: "",
    service_account: "",
  };
  if (isDestination) {
    return {
      ...initialVals,
      target_schema: "",
      gcs_bucket_name: "",
      gcs_bucket_path: "",
    };
  }
  return initialVals;
};
