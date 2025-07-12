import {
  Typography,
  Box,
  Button,
  InputLabel,
  Select,
  MenuItem,
  Grid,
  Divider,
  Stack,
  FormControl,
  FormHelperText,
} from "@mui/material";
import { Form, Formik } from "formik";
import * as Yup from "yup";
import useSWR from "swr";
import useSWRMutation from "swr/mutation";
import Link from "next/link";
import { ChevronLeft } from "@mui/icons-material";
import { useRouter } from "next/navigation";

import FormTextInput from "@/app/components/forms/FormTextInput";
import FormSelect from "@/app/components/forms/FormSelect";
import { getRequest, postRequest, pollTaskStatus } from "@/app/api/requests";

const fetchSourceMetadataWithPolling = async (key, { arg: sourceId }) => {
  if (!sourceId) return;
  try {
    console.log(`Starting metadata fetch for sourceId ${sourceId}`);
    const task = await postRequest(`/sources/${sourceId}/metadata`, {
      arg: {},
    });

    const result = await pollTaskStatus(
      `/sources/${sourceId}/metadata/${task.task_id}`
    );
    console.log(`Metadata fetched for sourceId ${sourceId}:`, result);

    if (
      result.success === undefined ||
      result.success === null ||
      result.success === false
    ) {
      throw new Error(
        "Error fetching metadata: Metadata was undefined or success was false"
      );
    }

    return result;
  } catch (error) {
    console.error("Error fetching metadata:", error);
    throw error;
  }
};

const AddModelForm = () => {
  const { data: sources, error, isLoading } = useSWR("/sources", getRequest);
  const filteredSources = sources?.filter((s) => s.state === "CREATED");
  const { trigger } = useSWRMutation("/models", postRequest);
  const router = useRouter();

  const {
    trigger: fetchMetadataTrigger,
    data: metadata,
    error: metadataFetchError,
    isMutating: isMetadataMutating,
  } = useSWRMutation(
    (sourceId) => `/sources/${sourceId}`,
    fetchSourceMetadataWithPolling
  );

  if (error) {
    return <Typography>Error with API</Typography>;
  }
  if (isLoading) {
    return <Typography>Loading screen!</Typography>;
  }

  return (
    <Box>
      <Button
        variant="contained"
        href="/models"
        component={Link}
        startIcon={<ChevronLeft />}
      >
        <Typography>Back</Typography>
      </Button>
      <Formik
        initialValues={{
          source: "",
          modelName: "",
          table: "",
          description: "",
          tenantIdCol: "",
          primaryKeyCol: "",
          lastModifiedCol: "",
        }}
        validationSchema={Yup.object({
          source: Yup.string().required("Required"),
          modelName: Yup.string()
            .max(64, "Must be 64 characters or less")
            .required("Required"),
          table: Yup.string().required("Required"),
          description: Yup.string().required("Required"),
          tenantIdCol: Yup.string().required("Required"),
          primaryKeyCol: Yup.string().required("Required"),
          lastModifiedCol: Yup.string().required("Required"),
        })}
        onSubmit={(values, { setSubmitting }) => {
          trigger({
            source_id: values.source,
            model_name: values.modelName,
            model_description: values.description,
            schema_name: values.table.split(".")[0],
            table_name: values.table.split(".")[1],
            include_columns: [],
            primary_key_column: values.primaryKeyCol,
            tenant_id_column: values.tenantIdCol,
            last_modified_at_column: values.lastModifiedCol,
          });

          router.push("/models");
          setSubmitting(false);
        }}
      >
        {({ values, setFieldValue }) => {
          const handleSourceChange = async (e) => {
            const selectedSource = e.target.value;
            setFieldValue("source", selectedSource);
            try {
              await fetchMetadataTrigger(selectedSource);
            } catch (error) {}
          };

          return (
            <Form>
              <Stack maxWidth={"sm"} spacing={3}>
                <Typography variant="h2" sx={{ paddingTop: "20px" }}>
                  Create Model
                </Typography>
                <FormSelect
                  id="source"
                  titleText="Source"
                  name="source"
                  onChange={handleSourceChange}
                  placeholder={
                    <Typography sx={{ fontStyle: "italic" }}>
                      Select Source
                    </Typography>
                  }
                >
                  {filteredSources.map((source, index) => (
                    <MenuItem
                      id={source.source_id}
                      value={source.source_id}
                      key={index}
                    >
                      {source.source_name}
                    </MenuItem>
                  ))}
                </FormSelect>
                {metadataFetchError ? (
                  <FormHelperText error>
                    Error: Failed to fetch tables from source
                  </FormHelperText>
                ) : null}
                <Divider />
                <ModelFormForSource
                  metadata={metadata}
                  metadataFetchError={metadataFetchError}
                  isMetadataMutating={isMetadataMutating}
                  tableValue={values.table}
                />
                <Button
                  type="submit"
                  variant="contained"
                  sx={{ width: "fit-content" }}
                >
                  Create
                </Button>
              </Stack>
            </Form>
          );
        }}
      </Formik>
    </Box>
  );
};

const getActiveColumns = (data, tableName) => {
  if (!data) {
    return null;
  }
  const targetObject = data.streams.find(
    (item) => `${item.schema_name}.${item.stream_name}` === tableName
  );
  return targetObject ? targetObject.fields : null;
};

const ModelFormForSource = ({
  metadata,
  metadataFetchError,
  isMetadataMutating,
  tableValue,
}) => {
  const data = metadata ? metadata.stream_info : undefined;
  const hasSourceData = data !== undefined;
  const isTableSelected = hasSourceData && tableValue;
  const activeColumns = getActiveColumns(data, tableValue);
  const tablePlaceholder =
    (metadataFetchError && "Error loading tables from Source") ||
    (isMetadataMutating && "Loading Tables From Source...") ||
    "Select Table";
  return (
    <>
      <FormSelect
        id="table"
        titleText="Table"
        name="table"
        disabled={!hasSourceData || metadataFetchError || isMetadataMutating}
        placeholder={
          <Typography sx={{ fontStyle: "italic" }}>
            {tablePlaceholder}
          </Typography>
        }
      >
        {hasSourceData &&
          data.streams.map((stream, index) => (
            <MenuItem
              key={index}
              id={stream.stream_name}
              value={`${stream.schema_name}.${stream.stream_name}`}
            >
              {`${stream.schema_name}.${stream.stream_name}`}
            </MenuItem>
          ))}
      </FormSelect>
      <FormTextInput
        label="Model Name"
        name="modelName"
        placeholder="MyModel"
        helperText="Descriptive name for this model"
        disabled={!isTableSelected}
      />

      <FormTextInput
        label="Description"
        name="description"
        placeholder="Description of the model"
        disabled={!isTableSelected}
      />

      <Divider />

      <FormSelect
        id="primaryKeyCol"
        titleText="Primary Key"
        name="primaryKeyCol"
        helperText="This column is a unique identifier for each row in the table."
        disabled={!isTableSelected}
      >
        {isTableSelected &&
          activeColumns.map((column, index) => (
            <MenuItem key={index} value={column.name}>
              {column.name}
            </MenuItem>
          ))}
      </FormSelect>

      <FormSelect
        id="lastModifiedCol"
        titleText="Last Modified Column"
        name="lastModifiedCol"
        helperText="This column indicates when a row was last updated and is used to determine whether the row should be included in the next sync."
        disabled={!isTableSelected}
      >
        {isTableSelected &&
          activeColumns.map((column, index) => (
            <MenuItem key={index} value={column.name}>
              {column.name}
            </MenuItem>
          ))}
      </FormSelect>

      <FormControl
        sx={{
          bgcolor: "warning.light",
          borderRadius: 2,
          padding: 1,
        }}
      >
        <FormSelect
          titleText="⚠️ Tenant ID Column"
          id="tenantIdCol"
          labelId="tenantIdCol-label"
          name="tenantIdCol"
          disabled={!isTableSelected}
        >
          {isTableSelected &&
            activeColumns.map((column, index) => (
              <MenuItem key={index} value={column.name}>
                {column.name}
              </MenuItem>
            ))}
        </FormSelect>

        <FormHelperText sx={{ ml: 0 }}>
          This column determines which rows are sent to which recipients.
        </FormHelperText>
      </FormControl>
    </>
  );
};

export default AddModelForm;
