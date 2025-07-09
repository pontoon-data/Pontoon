import {
  Typography,
  Box,
  Button,
  MenuItem,
  Stack,
  CircularProgress,
  Divider,
  FormHelperText,
} from "@mui/material";
import { useState } from "react";
import { Form, Formik } from "formik";
import * as Yup from "yup";
import useSWR from "swr";
import useSWRMutation from "swr/mutation";
import Link from "next/link";
import { ChevronLeft } from "@mui/icons-material";
import { useRouter } from "next/navigation";

import FormTextInput from "@/app/(DashboardLayout)/components/forms/FormTextInput";
import FormSelect from "@/app/(DashboardLayout)/components/forms/FormSelect";

import {
  SnowflakeConnectionDetails,
  getSnowflakeValidation,
  getSnowflakeInitialValues,
} from "@/app/(DashboardLayout)/components/forms/connection-details/SnowflakeConnectionDetails";
import {
  BigQueryConnectionDetails,
  getBigQueryValidation,
  getBigQueryInitialValues,
} from "@/app/(DashboardLayout)/components/forms/connection-details/BigQueryConnectionDetails";
import {
  RedshiftConnectionDetails,
  getRedshiftValidation,
  getRedshiftInitialValues,
} from "@/app/(DashboardLayout)/components/forms/connection-details/RedshiftConnectionDetails";
import {
  PostgresConnectionDetails,
  getPostgresValidation,
  getPostgresInitialValues,
} from "@/app/(DashboardLayout)/components/forms/connection-details/PostgresConnectionDetails";
import {
  getRequest,
  postRequest,
  putRequest,
  pollTaskStatus,
} from "@/app/api/requests";

const AddSourceForm = () => {
  const Status = Object.freeze({
    NOT_STARTED: 0,
    LOADING: 1,
    SUCCESS: 2,
    FAILED: 3,
  });

  // After refreshing the page, making the first API call resets the form.
  // In order to avoid the refresh happening during form validation, this api call is made here,
  // even though it is not used for anything
  const {
    data: sources,
    error: isError,
    isLoading,
  } = useSWR("/sources", getRequest);

  // state of the connection test
  const [testConnectionStatus, setTestConnectionStatus] = useState(
    Status.NOT_STARTED
  );

  // state for the source being created
  const [sourceCreated, setSourceCreated] = useState(false);
  const [sourceId, setSourceId] = useState("");

  const router = useRouter();

  const createSource = (params) => {
    return postRequest("/sources", { arg: params });
  };

  const updateSource = (sourceId, params) => {
    return putRequest(`/sources/${sourceId}`, { arg: params });
  };

  const startSourceCheck = (sourceId) => {
    return postRequest(`/sources/${sourceId}/check`, { arg: {} });
  };

  const waitForSourceCheck = (sourceId, taskId) => {
    return pollTaskStatus(`/sources/${sourceId}/check/${taskId}`);
  };

  const updateCheckState = (result) => {
    if (!result.success || result.success === false) {
      setTestConnectionStatus(Status.FAILED);
    } else {
      setTestConnectionStatus(Status.SUCCESS);
    }
  };

  // form submission handler
  const handleCreateAndCheckSource = async (values, validateForm) => {
    setTestConnectionStatus(Status.LOADING);
    const errors = await validateForm(values);
    if (Object.keys(errors).length > 0) {
      console.log(errors);
      setTestConnectionStatus(Status.FAILED);
      return;
    }
    const params = {
      source_name: values.source_name,
      vendor_type: values.vendor_type,
      connection_info: {
        vendor_type: values.vendor_type,
        auth_type: "basic",
        ...values[values.vendor_type],
      },
    };

    try {
      if (!sourceCreated) {
        console.log("Creating source");
        const result = await createSource(params);
        const check = await startSourceCheck(result.source_id);
        const status = await waitForSourceCheck(
          result.source_id,
          check.task_id
        );

        setSourceCreated(true);
        setSourceId(result.source_id);
        updateCheckState(status);
      } else {
        console.log("Updating source");
        await updateSource(sourceId, params);
        const check = await startSourceCheck(sourceId);
        const status = await waitForSourceCheck(sourceId, check.task_id);
        updateCheckState(status);
      }
    } catch (e) {
      setTestConnectionStatus(Status.FAILED);
    } finally {
    }
  };

  // when the create button is clicked
  const handleEnableSource = async (values) => {
    console.log("Submitting form");
    if (sourceCreated) {
      try {
        await updateSource(sourceId, { is_enabled: true, state: "CREATED" });
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
                      testConnectionStatus == Status.LOADING ||
                      !isValid ||
                      !dirty
                    }
                    onClick={() =>
                      handleCreateAndCheckSource(values, validateForm)
                    }
                  >
                    Test Connection
                  </Button>
                  {testConnectionStatus == Status.LOADING ? (
                    <CircularProgress size={28} />
                  ) : null}
                  {testConnectionStatus == Status.SUCCESS ? (
                    <Typography fontSize={26}>✅</Typography>
                  ) : null}
                  {testConnectionStatus == Status.FAILED ? (
                    <Typography fontSize={26}>❌</Typography>
                  ) : null}
                </Stack>
                <FormHelperText>
                  Testing the connection may take up to a minute.
                </FormHelperText>
              </Stack>
              <Button
                type="submit"
                variant="contained"
                disabled={
                  isValidating ||
                  !(testConnectionStatus == Status.SUCCESS) ||
                  !isValid ||
                  !dirty
                }
                sx={{
                  width: "fit-content",
                }}
              >
                Create
              </Button>
            </Stack>
          </Form>
        )}
      </Formik>
    </Box>
  );
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
