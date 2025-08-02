import {
  Typography,
  Box,
  Button,
  MenuItem,
  Stack,
  CircularProgress,
  Divider,
  FormHelperText,
  Alert,
} from "@mui/material";
import { Form, Formik } from "formik";
import * as Yup from "yup";
import { mutate } from "swr";
import useSWRMutation from "swr/mutation";
import Link from "next/link";
import { ChevronLeft } from "@mui/icons-material";
import { useRouter } from "next/navigation";

import FormTextInput from "@/app/components/forms/FormTextInput";
import FormSelect from "@/app/components/forms/FormSelect";

import {
  SnowflakeConnectionDetails,
  getSnowflakeValidation,
  getSnowflakeInitialValues,
} from "@/app/components/forms/connection-details/SnowflakeConnectionDetails";
import {
  BigQueryConnectionDetails,
  getBigQueryValidation,
  getBigQueryInitialValues,
} from "@/app/components/forms/connection-details/BigQueryConnectionDetails";
import {
  RedshiftConnectionDetails,
  getRedshiftValidation,
  getRedshiftInitialValues,
} from "@/app/components/forms/connection-details/RedshiftConnectionDetails";
import {
  PostgresConnectionDetails,
  getPostgresValidation,
  getPostgresInitialValues,
} from "@/app/components/forms/connection-details/PostgresConnectionDetails";
import { postRequest, putRequest, pollTaskStatus } from "@/app/api/requests";

const testConnection = async (key, { arg: values, sourceId }) => {
  const params = {
    source_name: values.source_name,
    vendor_type: values.vendor_type,
    connection_info: {
      vendor_type: values.vendor_type,
      ...values[values.vendor_type],
    },
  };

  try {
    if (!sourceId) {
      console.log("Creating source");
      const result = await postRequest("/sources", { arg: params });
      const check = await postRequest(`/sources/${result.source_id}/check`, {
        arg: {},
      });
      const status = await pollTaskStatus(
        `/sources/${result.source_id}/check/${check.task_id}`
      );

      if (
        status.success === undefined ||
        status.success === null ||
        status.success === false
      ) {
        throw new Error(status.message);
      }

      return {
        source_id: result.source_id,
        success: status.success,
      };
    } else {
      console.log("Updating source");
      const result = await putRequest(`/sources/${sourceId}`, { arg: params });
      const check = await postRequest(`/sources/${sourceId}/check`, {
        arg: {},
      });
      const status = await pollTaskStatus(
        `/sources/${sourceId}/check/${check.task_id}`
      );

      if (
        status.success === undefined ||
        status.success === null ||
        status.success === false
      ) {
        throw new Error(status.message);
      }

      return {
        source_id: result.source_id,
        success: status.success,
      };
    }
  } catch (e) {
    console.warn("Error testing connection: ", e);
    throw e;
  }
};

const enableSource = async (key, { arg: sourceId }) => {
  try {
    const result = await putRequest(`/sources/${sourceId}`, {
      arg: { is_enabled: true, state: "CREATED" },
    });
    // Invalidate the sources cache since there's a new source
    mutate("/sources");
    return result;
  } catch (e) {
    console.warn("Error enabling source: ", e);
    throw e;
  }
};

const AddSourceForm = () => {
  const {
    trigger: testConnectionTrigger,
    data: testConnectionResult,
    error: testConnectionError,
    isMutating: isTestConnectionMutating,
  } = useSWRMutation("/sources/test_connection", testConnection);
  const sourceId = testConnectionResult?.source_id;

  const {
    trigger: enableSourceTrigger,
    data: enableSourceResult,
    error: enableSourceError,
    isMutating: isEnableSourceMutating,
  } = useSWRMutation((sourceId) => `/sources/${sourceId}`, enableSource);

  const router = useRouter();

  // form submission handler
  const handleCreateAndCheckSource = async (values, validateForm) => {
    const errors = await validateForm(values);
    if (Object.keys(errors).length > 0) {
      console.log(errors);
      return;
    }

    try {
      const result = await testConnectionTrigger(values, sourceId);
    } catch (e) {
      console.warn("Error testing connection: ", e);
      return;
    }
  };

  // when the create button is clicked
  const handleEnableSource = async (values) => {
    console.log("Submitting form");
    if (sourceId) {
      try {
        await enableSourceTrigger(sourceId);
        router.push("/sources");
      } catch (e) {
        console.log("Enabling source failed: ", e);
      }
    }
  };

  return (
    <Box>
      <Button
        variant="contained"
        href="/sources"
        component={Link}
        startIcon={<ChevronLeft />}
      >
        <Typography>Back</Typography>
      </Button>
      <Formik
        initialValues={{
          source_name: "",
          vendor_type: "snowflake",
          snowflake: getSnowflakeInitialValues(false),
          redshift: getRedshiftInitialValues(false),
          bigquery: getBigQueryInitialValues(false),
          postgresql: getPostgresInitialValues(false),
        }}
        validationSchema={Yup.object({
          source_name: Yup.string()
            .max(64, "Must be 64 characters or less")
            .required("Required"),
          vendor_type: Yup.string()
            .oneOf(
              ["snowflake", "redshift", "bigquery", "postgresql"],
              "Invalid source type"
            )
            .required("Required"),

          snowflake: Yup.lazy((_, { parent }) =>
            parent.vendor_type === "snowflake"
              ? getSnowflakeValidation(false).required("Required")
              : Yup.mixed().notRequired()
          ),
          redshift: Yup.lazy((_, { parent }) =>
            parent.vendor_type === "redshift"
              ? getRedshiftValidation(false).required("Required")
              : Yup.mixed().notRequired()
          ),
          bigquery: Yup.lazy((_, { parent }) =>
            parent.vendor_type === "bigquery"
              ? getBigQueryValidation(false).required("Required")
              : Yup.mixed().notRequired()
          ),
          postgresql: Yup.lazy((_, { parent }) =>
            parent.vendor_type === "postgresql"
              ? getPostgresValidation(false).required("Required")
              : Yup.mixed().notRequired()
          ),
        })}
        onSubmit={handleEnableSource}
      >
        {({ values, isValidating, validateForm, isValid, dirty }) => (
          <Form>
            <Stack maxWidth={"sm"} spacing={3}>
              <Typography variant="h2" sx={{ paddingTop: "20px" }}>
                Create Source
              </Typography>
              <FormSelect
                id="vendor_type"
                titleText="Vendor type"
                name="vendor_type"
              >
                <MenuItem value={"snowflake"} selected>
                  Snowflake
                </MenuItem>
                <MenuItem value={"redshift"}>Redshift</MenuItem>
                <MenuItem value={"bigquery"}>BigQuery</MenuItem>
                <MenuItem value={"postgresql"}>Postgres</MenuItem>
              </FormSelect>
              <FormTextInput
                label="Source Name"
                name="source_name"
                placeholder="MySource"
                helperText="Descriptive name for this source"
              />
              <Divider />
              {renderConnectionDetails(values.vendor_type)}
              <Stack>
                <Stack direction="row" alignItems="center" spacing={1.5}>
                  <Button
                    type="button"
                    variant="contained"
                    disabled={
                      isValidating ||
                      isTestConnectionMutating ||
                      isEnableSourceMutating ||
                      !isValid ||
                      !dirty
                    }
                    onClick={() =>
                      handleCreateAndCheckSource(values, validateForm)
                    }
                  >
                    Test Connection
                  </Button>
                  {renderTestConnectionStatus(
                    testConnectionResult,
                    testConnectionError,
                    isTestConnectionMutating
                  )}
                </Stack>
                <FormHelperText>
                  Testing the connection may take up to a minute.
                </FormHelperText>
                {testConnectionError ? (
                  <Alert severity="error">
                    <Typography>{testConnectionError.message}</Typography>
                  </Alert>
                ) : null}
                {enableSourceError ? (
                  <Alert severity="error">
                    <Typography>{enableSourceError.message}</Typography>
                  </Alert>
                ) : null}
              </Stack>
              <Stack direction="row" alignItems="center" spacing={1.5}>
                <Button
                  type="submit"
                  variant="contained"
                  disabled={
                    isValidating ||
                    isTestConnectionMutating ||
                    isEnableSourceMutating ||
                    testConnectionError ||
                    testConnectionResult?.success === false ||
                    !isValid ||
                    !dirty
                  }
                  sx={{
                    width: "fit-content",
                  }}
                >
                  Create
                </Button>
                {isEnableSourceMutating ? <CircularProgress size={28} /> : null}
              </Stack>
            </Stack>
          </Form>
        )}
      </Formik>
    </Box>
  );
};

const renderTestConnectionStatus = (
  testConnectionResult,
  testConnectionError,
  isTestConnectionMutating
) => {
  if (isTestConnectionMutating) {
    return <CircularProgress size={28} />;
  }
  if (testConnectionError) {
    return <Typography fontSize={26}>❌</Typography>;
  }
  if (testConnectionResult?.success === true) {
    return <Typography fontSize={26}>✅</Typography>;
  }
  return null;
};

const renderConnectionDetails = (vendor_type) => {
  switch (vendor_type) {
    case "snowflake":
      return <SnowflakeConnectionDetails />;
    case "bigquery":
      return <BigQueryConnectionDetails />;
    case "redshift":
      return <RedshiftConnectionDetails />;
    case "postgresql":
      return <PostgresConnectionDetails />;
    default:
      return <Typography>Error: Source type not supported</Typography>;
  }
};

export default AddSourceForm;
