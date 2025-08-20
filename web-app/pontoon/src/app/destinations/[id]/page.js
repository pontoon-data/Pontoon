"use client";
import {
  Alert,
  Typography,
  Button,
  Stack,
  Box,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Tooltip,
  IconButton,
  CircularProgress,
  LinearProgress,
  RadioGroup,
  FormControlLabel,
  Radio,
} from "@mui/material";
import Snackbar from "@mui/material/Snackbar";
import { IconDotsVertical } from "@tabler/icons-react";
import useSWR from "swr";
import DashboardCard from "@/app//components/shared/DashboardCard";
import { ChevronLeft } from "@mui/icons-material";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import ScheduleIcon from "@mui/icons-material/Schedule";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import ErrorIcon from "@mui/icons-material/Error";
import ReplayIcon from "@mui/icons-material/Replay";
import AutorenewIcon from "@mui/icons-material/Autorenew";
import Link from "next/link";
import ListTable from "@/app/components/shared/ListTable";
import useSWRMutation from "swr/mutation";
import { useRouter, useParams } from "next/navigation";
import Tab from "@mui/material/Tab";
import TabContext from "@mui/lab/TabContext";
import TabList from "@mui/lab/TabList";
import TabPanel from "@mui/lab/TabPanel";
import { useState } from "react";
import { useTheme } from "@mui/material/styles";
import _ from "lodash";
import dayjs from "dayjs";
import LocalizedFormat from "dayjs/plugin/localizedFormat";
import Duration from "dayjs/plugin/duration";
import RelativeTime from "dayjs/plugin/relativeTime";
import timezone from "dayjs/plugin/timezone";
import advancedFormat from "dayjs/plugin/advancedFormat";
import utc from "dayjs/plugin/utc";
import TableBodyWrapper from "@/app/components/shared/TableBodyWrapper";
import { getScheduleText, getNextRunTime } from "@/utils/common";
import {
  getRequest,
  deleteRequest,
  rerunTransferRequest,
  runDestinationRequest,
} from "@/app/api/requests";
import { Form, Formik } from "formik";
import * as Yup from "yup";
import FormRadioGroup from "@/app/components/forms/FormRadioGroup";
import { DateTimePicker } from "@mui/x-date-pickers/DateTimePicker";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";

dayjs.extend(LocalizedFormat);
dayjs.extend(RelativeTime);
dayjs.extend(Duration);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);
dayjs.extend(utc);

const getDataForTable = (destinationData, recipientData, modelsData) => {
  const destination = destinationData;
  const filteredModels = modelsData
    .filter((m) => destinationData.models.includes(m.model_id))
    .map((m) => m.model_name)
    .join(", ");
  switch (destination.vendor_type) {
    case "snowflake":
      const snowflake = destination.connection_info;
      return [
        [
          "Recipient",
          `${recipientData.recipient_name} (${recipientData.tenant_id})`,
        ],
        ["Models", filteredModels],
        ["Vendor", "Snowflake"],
        ["Account", snowflake.account],
        ["Warehouse", snowflake.warehouse],
        ["Schema", snowflake.target_schema],
        ["Username", snowflake.user],
        ["Created", dayjs(destination.created_at).format("LLL z").toString()],
        ["Updated", dayjs(destination.modified_at).format("LLL z").toString()],
      ];
    case "bigquery":
      const bq = destination.connection_info;
      return [
        [
          "Recipient",
          `${recipientData.recipient_name} (${recipientData.tenant_id})`,
        ],
        ["Models", filteredModels],
        ["Vendor", "BigQuery"],
        ["Project ID", bq.project_id],
        ["Dataset", bq.dataset],
        ["GCS Bucket", bq.gcs_bucket],
        ["GCS Prefix", bq.gcs_prefix],
        ["Schema", bq.target_schema],
        [
          "Created At",
          dayjs(destination.created_at).format("LLL z").toString(),
        ],
        ["Updated", dayjs(destination.modified_at).format("LLL z").toString()],
      ];
    case "redshift":
      const redshift = destination.connection_info;
      return [
        [
          "Recipient",
          `${recipientData.recipient_name} (${recipientData.tenant_id})`,
        ],
        ["Vendor", "Redshift"],
        ["Models", filteredModels],
        ["Host Name", redshift.host],
        ["Port", redshift.port],
        ["Database", redshift.database],
        ["S3 Region", redshift.s3_region],
        ["S3 Bucket", redshift.s3_bucket],
        ["S3 Prefix", redshift.s3_prefix],
        ["IAM Role", redshift.iam_role],
        ["Schema", redshift.target_schema],
        ["Username", redshift.user],
        [
          "Created At",
          dayjs(destination.created_at).format("LLL z").toString(),
        ],
        ["Updated", dayjs(destination.modified_at).format("LLL z").toString()],
      ];
    case "postgresql":
      const postgresql = destination.connection_info;
      return [
        [
          "Recipient",
          `${recipientData.recipient_name} (${recipientData.tenant_id})`,
        ],
        ["Models", filteredModels],
        ["Vendor", "Postgres"],
        ["Host Name", postgresql.host],
        ["Port", postgresql.port],
        ["Database", postgresql.database],
        ["Schema", postgresql.target_schema],
        ["Username", postgresql.user],
        [
          "Created At",
          dayjs(destination.created_at).format("LLL z").toString(),
        ],
        ["Updated", dayjs(destination.modified_at).format("LLL z").toString()],
      ];
    default:
      console.log("Error reading data for details");
      return [];
  }
};

const DestinationDetails = () => {
  const params = useParams();
  const { id } = params;
  const {
    data: destination,
    error: destinationError,
    isLoading: destinationLoading,
  } = useSWR(`/destinations/${id}`, getRequest);
  const {
    data: recipient,
    error: recipientError,
    isLoading: recipientLoading,
  } = useSWR(() => `/recipients/${destination.recipient_id}`, getRequest);
  const {
    data: models,
    error: modelsError,
    isLoading: modelsLoading,
  } = useSWR(() => `/models`, getRequest);
  const { trigger: triggerDelete } = useSWRMutation(
    `/destinations/${id}`,
    deleteRequest
  );
  const router = useRouter();
  const [tab, setTab] = useState("1");
  const [openSuccess, setOpenSuccess] = useState(false);

  const handleTabChange = (event, newValue) => {
    setTab(newValue);
  };

  if (destinationError || recipientError || modelsError) {
    return <Typography>Error with API</Typography>;
  }
  if (destinationLoading || recipientLoading || modelsLoading) {
    return (
      <Box sx={{ width: "100%" }}>
        <LinearProgress color="inherit" />
      </Box>
    );
  }

  const dataForTable = getDataForTable(destination, recipient, models);

  return (
    <DashboardCard
      title={destination.destination_name}
      subtitle={getScheduleText(destination.schedule)}
      topContent={
        <Button
          variant="contained"
          href="/destinations"
          component={Link}
          sx={{ marginBottom: "24px" }}
          startIcon={<ChevronLeft />}
        >
          <Typography>Back</Typography>
        </Button>
      }
    >
      <Snackbar
        open={openSuccess}
        onClose={() => setOpenSuccess(false)}
        anchorOrigin={{ vertical: "top", horizontal: "center" }}
        autoHideDuration={6000}
        severity="success"
      >
        <Alert
          onClose={() => setOpenSuccess(false)}
          severity="success"
          variant="filled"
        >
          Transfer started
        </Alert>
      </Snackbar>

      <TabContext value={tab}>
        <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
          <TabList onChange={handleTabChange} aria-label="lab API tabs example">
            <Tab label="Transfers" value="1" />
            <Tab label="Backfill" value="2" />
            <Tab label="Details" value="3" />
          </TabList>
        </Box>
        <TabPanel value="1">
          <TransferTable
            schedule={destination.schedule}
            id={destination.destination_id}
            setOpenSuccess={setOpenSuccess}
          />
        </TabPanel>
        <TabPanel value="2">
          <BackfillPage setOpenSuccess={setOpenSuccess} setTab={setTab} />
        </TabPanel>
        <TabPanel value="3">
          <ListTable data={dataForTable} />
          <Stack direction="row" spacing={3} marginTop="20px">
            <Button
              variant="contained"
              startIcon={<DeleteIcon />}
              color="error"
              onClick={() => {
                triggerDelete();
                router.push("/destinations");
              }}
            >
              Delete
            </Button>
          </Stack>
        </TabPanel>
      </TabContext>
    </DashboardCard>
  );
};

const BackfillPage = ({ setOpenSuccess, setTab }) => {
  const params = useParams();
  const { id } = params;
  const { trigger: triggerRunDestination } = useSWRMutation(
    `/destinations/${id}/run`,
    runDestinationRequest
  );
  const runDestination = async (destinationId, values) => {
    // Convert dayjs objects to ISO strings to avoid Server Function serialization issues
    const scheduleOverride = {
      backfillType: values.backfillType,
      startTime: values.startTime ? values.startTime.toISOString() : null,
      endTime: values.endTime ? values.endTime.toISOString() : null,
    };

    triggerRunDestination({
      destinationId,
      scheduleOverride,
    });
    setOpenSuccess(true);
    setTab("1");
    // mutateTransfers();
  };
  return (
    <>
      <Formik
        initialValues={{
          backfillType: "",
          startTime: dayjs().subtract(1, "day"),
          endTime: dayjs(),
        }}
        validationSchema={Yup.object().shape({
          backfillType: Yup.string()
            .oneOf(["FULL_REFRESH", "INCREMENTAL"])
            .required("Backfill type is required"),
          startTime: Yup.date().when("backfillType", {
            is: "INCREMENTAL",
            then: (schema) =>
              schema.required(
                "Start time is required for incremental backfill"
              ),
            otherwise: (schema) => schema.nullable(),
          }),
          endTime: Yup.date()
            .when("backfillType", {
              is: "INCREMENTAL",
              then: (schema) =>
                schema.required(
                  "End time is required for incremental backfill"
                ),
              otherwise: (schema) => schema.nullable(),
            })
            .test(
              "end-after-start",
              "End time must be after start time",
              function (value) {
                const { startTime } = this.parent;
                if (startTime && value) {
                  return dayjs(value).isAfter(dayjs(startTime));
                }
                return true;
              }
            ),
        })}
        onSubmit={(values) => {
          runDestination(id, values);
        }}
      >
        {({ values, errors, touched, setFieldValue }) => (
          <Form>
            <Stack direction="row" spacing={3}>
              <FormRadioGroup
                name="backfillType"
                titleText="Backfill Type"
                options={[
                  { value: "FULL_REFRESH", label: "Full Refresh" },
                  { value: "INCREMENTAL", label: "Incremental" },
                ]}
              />
            </Stack>

            {values.backfillType === "INCREMENTAL" && (
              <Stack spacing={3} sx={{ mt: 3 }}>
                <Typography sx={{ fontWeight: 600, marginBottom: "4px" }}>
                  Incremental Load Settings
                </Typography>
                <Stack direction="row" spacing={3}>
                  <LocalizationProvider dateAdapter={AdapterDayjs}>
                    <DateTimePicker
                      label="Start Time"
                      value={values.startTime}
                      onChange={(newValue) =>
                        setFieldValue("startTime", newValue)
                      }
                      slotProps={{
                        textField: {
                          error: touched.startTime && Boolean(errors.startTime),
                          helperText: touched.startTime && errors.startTime,
                          fullWidth: true,
                        },
                      }}
                    />
                    <DateTimePicker
                      label="End Time"
                      value={values.endTime}
                      onChange={(newValue) =>
                        setFieldValue("endTime", newValue)
                      }
                      slotProps={{
                        textField: {
                          error: touched.endTime && Boolean(errors.endTime),
                          helperText: touched.endTime && errors.endTime,
                          fullWidth: true,
                        },
                      }}
                    />
                  </LocalizationProvider>
                </Stack>
              </Stack>
            )}

            <Button
              type="submit"
              variant="contained"
              color="primary"
              sx={{ mt: 2 }}
              disabled={!values.backfillType}
            >
              Run Backfill
            </Button>
          </Form>
        )}
      </Formik>
    </>
  );
};

const TransferTable = ({ schedule, id, setOpenSuccess }) => {
  const theme = useTheme();
  const router = useRouter();

  const {
    data: transfers,
    error: transfersError,
    isLoading: transfersLoading,
    isValidating: transfersValidating,
    mutate: mutateTransfers,
  } = useSWR(`/transfers?destination_id=${id}`, getRequest, {
    refreshInterval: 3000,
  });

  const { trigger: triggerRerunTransfer } = useSWRMutation(
    `/transfers/:id/rerun`,
    rerunTransferRequest
  );

  const { trigger: triggerRunDestination } = useSWRMutation(
    `/destinations/:id/run`,
    runDestinationRequest
  );

  const rerunTransfer = async (transferRunId) => {
    triggerRerunTransfer(transferRunId);
    setOpenSuccess(true);
    mutateTransfers();
  };

  const runDestination = async (destinationId) => {
    triggerRunDestination({
      destinationId,
      scheduleOverride: null,
    });
    setOpenSuccess(true);
    mutateTransfers();
    // autoRefresh();
  };

  const flattenTransferRuns = (transfers) => {
    const flatTransfers = [];
    const executionsMap = new Map();

    // Add a "virtual" placeholder for the next scheduled transfer
    flatTransfers.push({
      status: "SCHEDULED",
      scheduled_at: getNextRunTime(schedule),
    });

    // Group transfer runs by execution ID
    transfers.forEach((transfer) => {
      const executionId = transfer.meta.execution_id;
      if (!executionsMap.has(executionId)) {
        executionsMap.set(executionId, []);
      }
      executionsMap.get(executionId).push(transfer);
    });

    // Process each execution group
    for (const transferFromExecutionGroup of executionsMap.values()) {
      // Get the most recent transfer from the execution group
      const mostRecentTransfer = transferFromExecutionGroup.reduce(
        (latest, item) => {
          if (!latest) return item;
          return new Date(item.modified_at) > new Date(latest.modified_at)
            ? item
            : latest;
        },
        null
      );

      flatTransfers.push(mostRecentTransfer);

      // const retryMaxAttempts = transfers[0].meta.retry_max_attempts;
      // const statuses = transfers.map((t) => t.status);

      // // Prioritize running transfers
      // const runningTransfer = transfers.find((t) => t.status === "RUNNING");
      // if (runningTransfer) {
      //   flatTransfers.push(runningTransfer);
      //   continue;
      // }

      // const successTransfer = transfers.find((t) => t.status === "SUCCESS");
      // const latestTransfer = transfers[transfers.length - 1];

      // if (transfers.length === retryMaxAttempts) {
      //   // All attempts used – show success if available, otherwise show latest failure
      //   flatTransfers.push(successTransfer || latestTransfer);
      // } else if (!successTransfer) {
      //   // Still retrying – mark latest attempt as retrying
      //   flatTransfers.push({ ...latestTransfer, status: "RETRYING" });
      // } else {
      //   // Successfully completed
      //   flatTransfers.push(successTransfer);
      // }
    }

    return flatTransfers;
  };

  const flatTransfers = transfers ? flattenTransferRuns(transfers) : [];

  if (transfersError) {
    return <Alert severity="error">Error with API</Alert>;
  }

  if (transfersLoading) {
    return <LinearProgress color="inherit" />;
  }

  return (
    <Stack>
      <Table
        aria-label="transfers"
        sx={{
          mt: 2,
        }}
      >
        <TableHead>
          <TableRow
            sx={{
              cursor: "default",
              borderBottom: "2px",
              borderColor: theme.palette.grey[100],
              borderBottomStyle: "solid",
              backgroundColor: theme.palette.grey[100],
            }}
          >
            <TableCell sx={{ borderTopLeftRadius: "5pt" }}>
              <Typography variant="subtitle2" fontWeight={600}></Typography>
            </TableCell>
            <TableCell>
              <Typography variant="subtitle2" fontWeight={600}>
                Status
              </Typography>
            </TableCell>
            <TableCell>
              <Typography variant="subtitle2" fontWeight={600}>
                Start Time
              </Typography>
            </TableCell>
            <TableCell>
              <Typography variant="subtitle2" fontWeight={600}>
                Completed Time
              </Typography>
            </TableCell>
            <TableCell>
              <Typography variant="subtitle2" fontWeight={600}>
                Duration
              </Typography>
            </TableCell>
            <TableCell>
              <Typography variant="subtitle2" fontWeight={600}>
                Data Transfer Interval
              </Typography>
            </TableCell>
            <TableCell
              sx={{
                padding: "0",
                borderTopRightRadius: "5pt",
                textAlign: "right",
                paddingRight: "2.5em",
              }}
            >
              {(transfersLoading || transfersValidating) && (
                <CircularProgress color="grey" size="21px" />
              )}
            </TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          <TableBodyWrapper
            isError={false}
            isLoading={false}
            numRows={4}
            numColumns={3}
          >
            {flatTransfers &&
              flatTransfers.map((transfer, idx) => (
                <TableRow
                  key={idx}
                  hover={true}
                  sx={{
                    cursor:
                      transfer.transfer_run_id &&
                      transfer.status !== "SCHEDULED"
                        ? "pointer"
                        : "default",
                  }}
                  onClick={() => {
                    if (
                      transfer.transfer_run_id &&
                      transfer.status !== "SCHEDULED"
                    ) {
                      router.push(`/transfers/${transfer.transfer_run_id}`);
                    }
                  }}
                >
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {transfer.status == "RUNNING" && (
                        <CircularProgress color="success" size="22px" />
                      )}
                      {transfer.status == "SUCCESS" && (
                        <CheckCircleIcon color="success" />
                      )}
                      {transfer.status == "FAILURE" && (
                        <ErrorIcon color="error" />
                      )}
                      {transfer.status == "RETRYING" && (
                        <ErrorIcon color="warning" />
                      )}
                      {transfer.status == "SCHEDULED" && <ScheduleIcon />}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      {_.capitalize(transfer.status.toLowerCase())}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography noWrap variant="subtitle2" fontWeight={600}>
                      {transfer.created_at
                        ? dayjs(transfer.created_at)
                            .format("MMM D, h:mm A z")
                            .toString()
                        : ""}
                      {transfer.scheduled_at
                        ? dayjs(transfer.scheduled_at)
                            .format("MMM D, h:mm A z")
                            .toString()
                        : ""}
                    </Typography>
                  </TableCell>

                  <TableCell>
                    <Typography noWrap variant="subtitle2" fontWeight={600}>
                      {transfer.status == "SUCCESS" ||
                      transfer.status == "FAILURE"
                        ? dayjs(transfer.modified_at)
                            .format("MMM D, h:mm A z")
                            .toString()
                        : ""}
                    </Typography>
                  </TableCell>

                  <TableCell>
                    <Typography noWrap variant="subtitle2" fontWeight={600}>
                      {transfer.status == "SUCCESS" ||
                      transfer.status == "FAILURE"
                        ? dayjs
                            .duration(
                              dayjs(transfer.modified_at).diff(
                                transfer.created_at
                              )
                            )
                            .humanize()
                        : ""}

                      {transfer.status == "RUNNING" ||
                      transfer.status == "RETRYING"
                        ? dayjs
                            .duration(
                              dayjs(new Date().toISOString()).diff(
                                transfer.created_at
                              )
                            )
                            .humanize()
                        : ""}
                    </Typography>
                  </TableCell>

                  <TableCell>
                    <Typography noWrap variant="subtitle2" fontWeight={600}>
                      {transfer?.meta?.arguments?.mode?.type === "FULL_REFRESH"
                        ? `Full Refresh at ${dayjs(transfer.created_at)
                            .format("MMM D, h:mm A z")
                            .toString()}`
                        : ""}
                      {transfer?.meta?.arguments?.mode?.type === "INCREMENTAL"
                        ? `${dayjs(transfer.meta.arguments.mode.start)
                            .format("MMM D, h:mm A z")
                            .toString()} - ${dayjs(
                            transfer.meta.arguments.mode.end
                          )
                            .format("MMM D, h:mm A z")
                            .toString()}`
                        : ""}
                    </Typography>
                  </TableCell>

                  <TableCell align="right">
                    {(transfer.status == "SUCCESS" ||
                      transfer.status == "FAILURE") && (
                      <Button
                        variant="outlined"
                        size="small"
                        disabled={false}
                        onClick={(e) => {
                          e.stopPropagation();
                          rerunTransfer(transfer.transfer_run_id);
                        }}
                      >
                        Re-run
                      </Button>
                    )}

                    {/* {transfer.status == "SCHEDULED" && (
                      <Button
                        variant="outlined"
                        size="small"
                        disabled={false}
                        onClick={(e) => {
                          e.stopPropagation();
                          runDestination(id);
                        }}
                      >
                        Run Now
                      </Button>
                    )} */}
                  </TableCell>
                </TableRow>
              ))}
          </TableBodyWrapper>
        </TableBody>
      </Table>
    </Stack>
  );
};

export default DestinationDetails;
