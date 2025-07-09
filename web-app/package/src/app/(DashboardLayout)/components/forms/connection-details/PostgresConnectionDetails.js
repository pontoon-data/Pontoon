import { Typography, Stack } from "@mui/material";
import * as Yup from "yup";

import FormTextInput from "@/app/(DashboardLayout)/components/forms/FormTextInput";

export const PostgresConnectionDetails = ({ isDestination }) => {
  return (
    <>
      {!isDestination ? (
        <Stack>
          <Typography sx={{ fontWeight: 600, marginBottom: "4px" }}>
            Enter the source credentials
          </Typography>
          <Typography>
            Provide the details to connect to this Postgres source.
          </Typography>
        </Stack>
      ) : null}
      <FormTextInput
        label="Hostname"
        name="postgresql.host"
        placeholder="mypostgres.example.com"
      />
      <FormTextInput
        label="Port"
        name="postgresql.port"
        type="number"
        placeholder="5432"
      />
      <FormTextInput
        label="Database"
        name="postgresql.database"
        placeholder="main"
      />
      <FormTextInput
        label="Username"
        name="postgresql.user"
        placeholder="data-transfer-user"
      />
      <FormTextInput
        label="Password"
        name="postgresql.password"
        type="password"
        autoComplete="current-password"
        placeholder="************"
      />
      {isDestination === true ? (
        <>
          <FormTextInput
            label="Destination Schema"
            name="postgresql.target_schema"
            placeholder="export"
          />
        </>
      ) : null}
    </>
  );
};

export const getPostgresValidation = (isDestination) => {
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
    });
    schema = schema.concat(destinationSchema);
  }

  return schema;
};

export const getPostgresInitialValues = (isDestination) => {
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
    };
  }
  return initialVals;
};
