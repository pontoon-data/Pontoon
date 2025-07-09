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
} from "@mui/material";
import Snackbar from "@mui/material/Snackbar";
import { IconDotsVertical } from "@tabler/icons-react";
import useSWR from "swr";
import DashboardCard from "@/app/(DashboardLayout)//components/shared/DashboardCard";
import { ChevronLeft } from "@mui/icons-material";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import ScheduleIcon from "@mui/icons-material/Schedule";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import ErrorIcon from "@mui/icons-material/Error";
import ReplayIcon from "@mui/icons-material/Replay";
import AutorenewIcon from "@mui/icons-material/Autorenew";
import Link from "next/link";
import ListTable from "@/app/(DashboardLayout)/components/shared/ListTable";
import useSWRMutation from "swr/mutation";
import { useRouter } from "next/navigation";
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
import TableBodyWrapper from "@/app/(DashboardLayout)/components/shared/TableBodyWrapper";
import { getScheduleText, getNextRunTime } from "@/utils/common";
import {
  getRequest,
  deleteRequest,
  rerunTransferRequest,
  runDestinationRequest,
} from "@/app/api/requests";

dayjs.extend(LocalizedFormat);
dayjs.extend(RelativeTime);
dayjs.extend(Duration);

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
        ["Created", dayjs(destination.created_at).format("LLL").toString()],
        ["Updated", dayjs(destination.modified_at).format("LLL").toString()],
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
        ["Created At", dayjs(destination.created_at).format("LLL").toString()],
        ["Updated", dayjs(destination.modified_at).format("LLL").toString()],
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
        ["Created At", dayjs(destination.created_at).format("LLL").toString()],
        ["Updated", dayjs(destination.modified_at).format("LLL").toString()],
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
        ["Created At", dayjs(destination.created_at).format("LLL").toString()],
        ["Updated", dayjs(destination.modified_at).format("LLL").toString()],
      ];
    default:
      console.log("Error reading data for details");
      return [];
  }
};

const DestinationDetails = ({ params }) => {
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
      <TabContext value={tab}>
        <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
          <TabList onChange={handleTabChange} aria-label="lab API tabs example">
            <Tab label="Transfers" value="1" />
            <Tab label="Details" value="2" />
          </TabList>
        </Box>
        <TabPanel value="1">
          <TransferTable
            schedule={destination.schedule}
            id={destination.destination_id}
          />
        </TabPanel>
        <TabPanel value="2">
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

const TransferTable = ({ schedule, id }) => {
  const theme = useTheme();

  const [openSuccess, setOpenSuccess] = useState(false);

  const {
    data: transfers,
    error: transfersError,
    isLoading: transfersLoading,
    isValidating: transfersValidating,
    mutate: mutateTransfers,
  } = useSWR(`/transfers?destination_id=${id}`, getRequest);

  const { trigger: triggerRerunTransfer } = useSWRMutation(
    `/transfers/:id/rerun`,
    rerunTransferRequest
  );

  const { trigger: triggerRunDestination } = useSWRMutation(
    `/destinations/:id/run`,
    runDestinationRequest
  );

  const refreshTransfers = () => {
    mutateTransfers();
  };

  const autoRefresh = () => {
    const intervalId = setInterval(refreshTransfers, 3000);
    // Stop after 5 min
    setTimeout(() => {
      clearInterval(intervalId);
    }, 60000 * 5);
  };

  const rerunTransfer = async (transferRunId) => {
    triggerRerunTransfer(transferRunId);
    setOpenSuccess(true);
    autoRefresh();
  };

  const runDestination = async (destinationId) => {
    triggerRunDestination(destinationId);
    setOpenSuccess(true);
    autoRefresh();
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
    for (const transfers of executionsMap.values()) {
      const retryMaxAttempts = transfers[0].meta.retry_max_attempts;
      const statuses = transfers.map((t) => t.status);

      // Prioritize running transfers
      const runningTransfer = transfers.find((t) => t.status === "RUNNING");
      if (runningTransfer) {
        flatTransfers.push(runningTransfer);
        continue;
      }

      const successTransfer = transfers.find((t) => t.status === "SUCCESS");
      const latestTransfer = transfers[transfers.length - 1];

      if (transfers.length === retryMaxAttempts) {
        // All attempts used – show success if available, otherwise show latest failure
        flatTransfers.push(successTransfer || latestTransfer);
      } else if (!successTransfer) {
        // Still retrying – mark latest attempt as retrying
        flatTransfers.push({ ...latestTransfer, status: "RETRYING" });
      } else {
        // Successfully completed
        flatTransfers.push(successTransfer);
      }
    }

    return flatTransfers;
  };

  return (
    <Stack>
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
            {(() => {
              if (!transfers) return;

              const flatTransfers = flattenTransferRuns(transfers);

              return flatTransfers.map((transfer, idx) => (
                <TableRow key={idx} hover={true} sx={{ cursor: "pointer" }}>
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
                        ? dayjs(transfer.created_at).format("LLL").toString()
                        : ""}
                      {transfer.scheduled_at
                        ? dayjs(transfer.scheduled_at).format("LLLL").toString()
                        : ""}
                    </Typography>
                  </TableCell>

                  <TableCell>
                    <Typography noWrap variant="subtitle2" fontWeight={600}>
                      {transfer.status == "SUCCESS" ||
                      transfer.status == "FAILURE"
                        ? dayjs(transfer.modified_at).format("LTS").toString()
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

                  <TableCell align="right">
                    {(transfer.status == "SUCCESS" ||
                      transfer.status == "FAILURE") && (
                      <Button
                        variant="outlined"
                        size="small"
                        disabled={false}
                        onClick={() => rerunTransfer(transfer.transfer_run_id)}
                      >
                        Re-run
                      </Button>
                    )}

                    {transfer.status == "SCHEDULED" && (
                      <Button
                        variant="outlined"
                        size="small"
                        disabled={false}
                        onClick={() => runDestination(id)}
                      >
                        Run Now
                      </Button>
                    )}
                    <Tooltip title="Options" placement="right">
                      <IconButton>
                        <IconDotsVertical />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ));
            })()}
          </TableBodyWrapper>
        </TableBody>
      </Table>
    </Stack>
  );
};

export default DestinationDetails;
