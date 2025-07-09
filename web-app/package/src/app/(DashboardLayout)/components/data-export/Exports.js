import {
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Chip,
  Button,
  Fab,
  IconButton,
  Tooltip,
  TextField,
  Select,
  MenuItem,
  FormHelperText,
  Grid,
  Stack,
  Checkbox,
} from "@mui/material";
import DashboardCard from "@/app/(DashboardLayout)//components/shared/DashboardCard";
import { IconDotsVertical, IconPlus, IconSettings } from "@tabler/icons-react";
import FormDialog from "./CreateExportDialog";
import { MouseEventHandler, useState } from "react";
import { ChevronLeft } from "@mui/icons-material";
import { ErrorMessage, Field, Form, Formik, useField, useFormik } from "formik";
import * as Yup from "yup";
import { uniqueId } from "lodash";
import { useTheme } from "@mui/material/styles";

const products = [
  {
    id: "1",
    destinationName: "Acme Demo S3",
    pbg: "success.main",
    destinationType: "Amazon S3",
    status: "OK",
    scheduledAt: "Daily @ 3:00 AM PST",
    createdAt: "Dec 2, 2024 1:04 PM PST",
    createdBy: "test-user",
    lastSuccessfulSync: "Dec 5, 2024 3:00 AM PST",
    recipientId: "Acme",
  },
  {
    id: "3",
    destinationName: "Acme Staging Redshift",
    pbg: "success.main",
    destinationType: "Redshift",
    status: "OK",
    scheduledAt: "Daily @ 2:00 AM PST",
    createdAt: "Dec 4, 2024 2:46 PM PST",
    createdBy: "data-team",
    lastSuccessfulSync: "Dec 5, 2024 3:00 AM PST",
    recipientId: "Acme",
  },
  {
    id: "4",
    destinationName: "Example Co Demo Snowflake",
    pbg: "warning.main",
    destinationType: "Snowflake",
    status: "Paused",
    scheduledAt: "Daily @ 2:00 AM PST",
    createdAt: "Dec 4, 2024 2:46 PM PST",
    createdBy: "data-team",
    lastSuccessfulSync: "Dec 5, 2024 3:00 AM PST",
    recipientId: "Example Co",
  },
];

const Recipients = () => {
  const [isAddingSource, setIsAddingSource] = useState(false);
  const handleClickAddSource = (event) => {
    setIsAddingSource(true);
  };
  const handleClickBack = (event) => {
    setIsAddingSource(false);
  };
  const theme = useTheme();
  const primary = theme.palette.primary.main;
  if (!isAddingSource) {
    return (
      <DashboardCard
        title="Destinations"
        action={
          <Button variant="contained" onClick={handleClickAddSource}>
            <IconPlus width={20} />
            <Typography sx={{ paddingLeft: "3px" }}>Add Destination</Typography>
          </Button>
        }
      >
        <Box sx={{ overflow: "auto", width: { xs: "280px", sm: "auto" } }}>
          <Table
            aria-label="simple table"
            sx={{
              // whiteSpace: "nowrap",
              mt: 2,
            }}
          >
            <TableHead>
              <TableRow
                sx={{
                  borderBottom: "2px",
                  borderColor: theme.palette.grey[100],
                  borderBottomStyle: "solid",
                  backgroundColor: theme.palette.grey[100],
                }}
              >
                <TableCell sx={{ borderTopLeftRadius: "5pt" }}>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Recipient
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Destination Name
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Destination
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Status
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Schedule
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Last Successful Sync
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Created Time
                  </Typography>
                </TableCell>
                <TableCell sx={{ borderTopRightRadius: "5pt" }} />
              </TableRow>
            </TableHead>
            <TableBody>
              {products.map((product) => (
                <TableRow key={product.name}>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {product.recipientId}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {product.destinationName}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {product.destinationType}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Chip
                      sx={{
                        px: "4px",
                        backgroundColor: product.pbg,
                        color: "#fff",
                      }}
                      size="small"
                      label={product.status}
                    ></Chip>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {product.scheduledAt}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {product.lastSuccessfulSync}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {product.createdAt}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Tooltip title="Options" placement="right">
                      <IconButton>
                        <IconDotsVertical />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>
      </DashboardCard>
    );
  } else {
    return (
      <DashboardCard>
        <Button variant="contained" onClick={handleClickBack}>
          <ChevronLeft width={20} />
          <Typography sx={{ paddingLeft: "3px" }}>Back</Typography>
        </Button>
        <AddModelForm handleClick={handleClickBack} />
      </DashboardCard>
    );
  }
};

const AddModelForm = (props) => {
  const [showTestConnectionCheck, setShowTestConnectionCheck] = useState(false);
  const [showTestConnectionLoading, setShowTestConnectionLoading] =
    useState(false);
  return (
    <Formik
      initialValues={{
        destinationType: "snowflake",
        destinationName: "",
        recipientId: "",
      }}
      validationSchema={Yup.object({
        destinationType: Yup.string()
          .oneOf(["Snowflake", "Redshift", "BigQuery"], "Invalid source type")
          .required("Required"),
        destinationName: Yup.string().required("Required"),
        recipientId: Yup.string().required("Required"),
        hostName: Yup.string().required("Required"),
        port: Yup.number()
          .integer("Must be a valid port number")
          .max(65535, "Must be a valid port number")
          .min(0, "Must be a valid port number")
          .required("Required"),
        database: Yup.string().required("Required"),
        schema: Yup.string().required("Required"),
        username: Yup.string().required("Required"),
        password: Yup.string().required("Required"),
      })}
      onSubmit={(values, { setSubmitting }) => {
        const currentTime = Date.now();
        let year = new Intl.DateTimeFormat("en", { year: "numeric" }).format(
          currentTime
        );
        let month = new Intl.DateTimeFormat("en", { month: "short" }).format(
          currentTime
        );
        let day = new Intl.DateTimeFormat("en", { day: "2-digit" }).format(
          currentTime
        );
        let time = new Intl.DateTimeFormat("en-US", {
          hour12: true,
          timeStyle: "short",
          timeZone: "PST",
        })
          .format(currentTime)
          .toUpperCase();
        const createdTime = `${month} ${day}, ${year} ${time} PST`;
        products.push({
          id: uniqueId(),
          status: "OK",
          pbg: "success.main",
          destinationType: values.destinationType,
          destinationName: values.destinationName,
          recipientId: values.recipientId,
          scheduledAt: "Daily @ 2:00 AM PST",
          createdAt: "Jan 8, 2024 4:16 PM PST",
          createdBy: "test-user",
          lastSuccessfulSync: "Jan 8, 2024 4:16 PM PST",
        });
        props.handleClick();
      }}
    >
      {({ values }) => (
        <Form>
          <Grid container maxWidth={"sm"} spacing={3}>
            <Grid item xs={12}>
              <Typography variant="h2" sx={{ marginTop: "20px" }}>
                Add Destination
              </Typography>
            </Grid>
            <Grid item xs={12}>
              <FormSelect
                id="destinationType"
                titleText="Destination type"
                name="destinationType"
              >
                <MenuItem value={"Snowflake"} selected>
                  Snowflake
                </MenuItem>
                <MenuItem value={"Redshift"}>Redshift</MenuItem>
                <MenuItem value={"BigQuery"}>BigQuery</MenuItem>
              </FormSelect>
            </Grid>
            <Grid item xs={12}>
              <FormTextInput
                label="Destination Name"
                name="destinationName"
                helperText="Descriptive and unique name for this destination. This is only visible internally and is only used as a reference."
              />
            </Grid>
            <Grid item xs={12}>
              <FormSelect
                id="recipientId"
                titleText="Recipient ID"
                name="recipientId"
                helperText="Choose a recipient. A recipient must be created before a destination can be added."
              >
                <MenuItem value={"Acme"}>Acme (acme)</MenuItem>
                <MenuItem value={"Example Co"}>Example Co (example)</MenuItem>
                <MenuItem value={"Test Industries"}>
                  Test Industries (testIndustries)
                </MenuItem>
              </FormSelect>
            </Grid>

            <Grid item xs={12}>
              <Typography sx={{ fontWeight: 600, marginBottom: "4px" }}>
                Enter the destination credentials
              </Typography>
            </Grid>

            <Grid item xs={12}>
              <FormTextInput
                label="Hostname"
                name="hostName"
                placeholder="abc123.us-west.snowflakecomputing.com"
              />
            </Grid>

            <Grid item xs={12}>
              <FormTextInput label="Port" name="port" placeholder="443" />
            </Grid>

            <Grid item xs={12}>
              <FormTextInput label="Database" name="database" />
            </Grid>

            <Grid item xs={12}>
              <FormTextInput label="Schema" name="schema" />
            </Grid>

            <Grid item xs={12}>
              <FormTextInput
                label="Username"
                name="username"
                placeholder="data-transfer-user"
              />
            </Grid>

            <Grid item xs={12}>
              <FormTextInput
                label="Password"
                name="password"
                type="password"
                autoComplete="current-password"
                placeholder="************"
              />
            </Grid>

            <Grid item xs={12}>
              <Typography>
                Select what models the destination will receive
              </Typography>
              <Stack direction="row" xs={12} alignItems="center">
                <Checkbox defaultChecked />
                <Typography sx={{ fontWeight: 600 }}>
                  All current and future models
                </Typography>
              </Stack>
              <Stack direction="row" xs={12} alignItems="center">
                <Checkbox disabled checked />
                <Typography sx={{ fontWeight: 600 }}>campaigns</Typography>
              </Stack>
              <Stack direction="row" xs={12} alignItems="center">
                <Checkbox disabled checked />
                <Typography sx={{ fontWeight: 600 }}>leads</Typography>
              </Stack>
              <Stack direction="row" xs={12} alignItems="center">
                <Checkbox disabled checked />
                <Typography sx={{ fontWeight: 600 }}>prospects</Typography>
              </Stack>
            </Grid>

            <Grid item xs={12}>
              <Stack direction="row" alignItems="center" spacing={1.5}>
                <Button
                  variant="contained"
                  onClick={() => {
                    setShowTestConnectionLoading(true);
                    setTimeout(() => {
                      setShowTestConnectionLoading(false);
                      setShowTestConnectionCheck(true);
                    }, 2000);
                  }}
                >
                  Test Connection
                </Button>
                {showTestConnectionLoading ? (
                  <Typography>Checking connection...</Typography>
                ) : null}
                {showTestConnectionCheck ? (
                  <Typography fontSize={26}>✅</Typography>
                ) : null}
              </Stack>
            </Grid>

            <Grid item xs={12}>
              <Button type="submit" variant="contained">
                Submit
              </Button>
            </Grid>
          </Grid>
        </Form>
      )}
    </Formik>
  );
};

const FormTextInput = ({ label, helperText, ...props }) => {
  // useField() returns [formik.getFieldProps(), formik.getFieldMeta()]
  // which we can spread on <input>. We can use field meta to show an error
  // message if the field is invalid and it has been touched (i.e. visited)
  const [field, meta] = useField(props);
  return (
    <Box>
      <Typography
        htmlFor={props.id || props.name}
        sx={{ fontWeight: 600, marginBottom: "4px" }}
      >
        {label}
      </Typography>
      <TextField {...field} {...props} fullWidth />
      {meta.touched && meta.error ? (
        <div className="error">{meta.error}</div>
      ) : null}
      <FormHelperText>{helperText}</FormHelperText>
    </Box>
  );
};

const FormSelect = ({ label, helperText, ...props }) => {
  // useField() returns [formik.getFieldProps(), formik.getFieldMeta()]
  // which we can spread on <input>. We can use field meta to show an error
  // message if the field is invalid and it has been touched (i.e. visited)
  const [field, meta] = useField(props);
  return (
    <>
      <Typography
        htmlFor={props.id || props.name}
        sx={{ fontWeight: 600, marginBottom: "4px" }}
      >
        {label}
      </Typography>
      <Select {...field} {...props} fullWidth />
      {meta.touched && meta.error ? (
        <div className="error">{meta.error}</div>
      ) : null}
      <FormHelperText>{helperText}</FormHelperText>
    </>
  );
};

export default Recipients;
