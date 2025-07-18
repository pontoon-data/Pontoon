import { Typography, Stack } from "@mui/material";
import * as Yup from "yup";

import FormTextInput from "@/app/components/forms/FormTextInput";

export const SnowflakeConnectionDetails = ({ isDestination }) => {
  return (
    <>
      {!isDestination ? (
        <Stack>
          <Typography sx={{ fontWeight: 600, marginBottom: "4px" }}>
            Enter the source credentials
          </Typography>
          <Typography>
            Provide the details to connect to this Snowflake source.
          </Typography>
        </Stack>
      ) : null}
      <FormTextInput
        label="Account"
        name="snowflake.account"
        placeholder="abc123"
      />
      <FormTextInput
        label="Warehouse"
        name="snowflake.warehouse"
        placeholder="primary"
      />
      <FormTextInput
        label="Database"
        name="snowflake.database"
        placeholder="primary"
      />
      {isDestination === true ? (
        <FormTextInput
          label="Destination Schema"
          name="snowflake.target_schema"
          placeholder="export"
        />
      ) : null}
      <FormTextInput
        label="Username"
        name="snowflake.user"
        placeholder="data-transfer-user"
      />
      <FormTextInput
        label="Access Token"
        name="snowflake.access_token"
        type="password"
        autoComplete="access-token"
        placeholder="************"
      />
    </>
  );
};

export const getSnowflakeValidation = (isDestination) => {
  let schema = Yup.object().shape({
    account: Yup.string().required("Required"),
    warehouse: Yup.string().required("Required"),
    database: Yup.string().required("Required"),
    user: Yup.string().required("Required"),
    access_token: Yup.string().required("Required"),
  });

  if (isDestination) {
    const destinationSchema = Yup.object().shape({
      target_schema: Yup.string().required("Required"),
    });
    schema = schema.concat(destinationSchema);
  }

  return schema;
};

export const getSnowflakeInitialValues = (isDestination) => {
  const initialVals = {
    account: "",
    warehouse: "",
    database: "",
    user: "",
    access_token: "",
  };
  if (isDestination) {
    return {
      ...initialVals,
      target_schema: "",
    };
  }
  return initialVals;
};
