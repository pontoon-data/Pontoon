import { Typography, Stack } from "@mui/material";
import * as Yup from "yup";

import FormTextInput from "@/app/components/forms/FormTextInput";

export const RedshiftConnectionDetails = ({ isDestination }) => {
  return (
    <>
      {!isDestination ? (
        <Stack>
          <Typography sx={{ fontWeight: 600, marginBottom: "4px" }}>
            Enter the source credentials
          </Typography>
          <Typography>
            Provide the details to connect to this Redshift source.
          </Typography>
        </Stack>
      ) : null}
      <FormTextInput
        label="Hostname"
        name="redshift.host"
        placeholder="mycluster.abc123.us-east-1.redshift.amazonaws.com"
      />
      <FormTextInput
        label="Port"
        name="redshift.port"
        type="number"
        placeholder="5439"
      />
      <FormTextInput
        label="Database"
        name="redshift.database"
        placeholder="main"
      />
      <FormTextInput
        label="Username"
        name="redshift.user"
        placeholder="data-transfer-user"
      />
      <FormTextInput
        label="Password"
        name="redshift.password"
        type="password"
        autoComplete="current-password"
        placeholder="************"
      />
      {isDestination === true ? (
        <>
          <FormTextInput
            label="Destination Schema"
            name="redshift.target_schema"
            placeholder="export"
          />
          <FormTextInput
            label="S3 Region"
            name="redshift.s3_region"
            placeholder="us-east-1"
          />
          <FormTextInput
            label="S3 Bucket"
            name="redshift.s3_bucket"
            placeholder="s3://mybucket"
          />
          <FormTextInput
            label="S3 Bucket Prefix"
            name="redshift.s3_prefix"
            placeholder="/exports"
          />
          <FormTextInput
            label="IAM Role"
            name="redshift.iam_role"
            placeholder="arn:aws:iam::<account-id>:role/<role-name>"
          />
          <FormTextInput
            label="AWS Access Key ID"
            name="redshift.aws_access_key_id"
            type="password"
            autoComplete="current-access-key-id"
            placeholder="************"
          />
          <FormTextInput
            label="AWS Secret Access Key"
            name="redshift.aws_secret_access_key"
            type="password"
            autoComplete="current-secret-access-key"
            placeholder="************"
          />
        </>
      ) : null}
    </>
  );
};

export const getRedshiftValidation = (isDestination) => {
  let schema = Yup.object().shape({
    host: Yup.string().required("Required"),
    port: Yup.number()
      .integer("Must be a valid port number")
      .max(65535, "Must be a valid port number")
      .min(0, "Must be a valid port number")
      .required("Required"),
    database: Yup.string().required("Required"),
    user: Yup.string().required("Required"),
    password: Yup.string().required("Required"),
  });

  if (isDestination) {
    const destinationSchema = Yup.object().shape({
      target_schema: Yup.string().required("Required"),
      s3_region: Yup.string().required("Required"),
      s3_bucket: Yup.string().required("Required"),
      s3_prefix: Yup.string().required("Required"),
      iam_role: Yup.string().required("Required"),
      aws_access_key_id: Yup.string().required("Required"),
      aws_secret_access_key: Yup.string().required("Required"),
    });
    schema = schema.concat(destinationSchema);
  }

  return schema;
};

export const getRedshiftInitialValues = (isDestination) => {
  const initialVals = {
    host: "",
    port: "",
    database: "",
    user: "",
    password: "",
  };
  if (isDestination) {
    return {
      ...initialVals,
      target_schema: "",
      s3_region: "",
      s3_bucket: "",
      s3_prefix: "",
      iam_role: "",
      aws_access_key_id: "",
      aws_secret_access_key: "",
    };
  }
  return initialVals;
};
