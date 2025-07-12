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
        label="Password"
        name="snowflake.password"
        type="password"
        autoComplete="current-password"
        placeholder="************"
      />
    </>
  );
};

export const getSnowflakeValidation = (isDestination) => {
  let schema = Yup.object().shape({
    account: Yup.string().required("Required"),
    warehouse: Yup.string().required("Required"),
    user: Yup.string().required("Required"),
    password: Yup.string().required("Required"),
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
    user: "",
    password: "",
  };
  if (isDestination) {
    return {
      ...initialVals,
      target_schema: "",
    };
  }
  return initialVals;
};
