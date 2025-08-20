"use client";
import {
  Alert,
  Typography,
  Button,
  Stack,
  Box,
  CircularProgress,
  LinearProgress,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  useTheme,
} from "@mui/material";
import { ChevronLeft } from "@mui/icons-material";
import useSWR from "swr";
import DashboardCard from "@/app/components/shared/DashboardCard";
import ListTable from "@/app/components/shared/ListTable";
import { useParams, useRouter } from "next/navigation";
import Link from "next/link";
import { getRequest } from "@/app/api/requests";
import dayjs from "dayjs";
import LocalizedFormat from "dayjs/plugin/localizedFormat";
import timezone from "dayjs/plugin/timezone";
import advancedFormat from "dayjs/plugin/advancedFormat";
import Duration from "dayjs/plugin/duration";
import RelativeTime from "dayjs/plugin/relativeTime";

dayjs.extend(LocalizedFormat);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);
dayjs.extend(Duration);
dayjs.extend(RelativeTime);

const TransferDetails = () => {
  const params = useParams();
  const { id } = params;
  const router = useRouter();
  const theme = useTheme();
  const {
    data: transferRun,
    error: transferRunError,
    isLoading: transferRunLoading,
  } = useSWR(`/transfers/${id}`, getRequest);
  const transfer_id = transferRun?.transfer_id;
  const execution_id = transferRun?.meta?.execution_id;

  const {
    data: transfer,
    error: transferError,
    isLoading: transferLoading,
  } = useSWR(
    transfer_id ? ["/transfers/transfer", transfer_id] : null,
    ([url, transfer_id]) => getRequest(`${url}/${transfer_id}`)
  );
  const destination_id = transfer?.destination_id;

  const {
    data: transferRunsWithSameExecutionId,
    error: transferRunsWithSameExecutionIdError,
    isLoading: transferRunsWithSameExecutionIdLoading,
  } = useSWR(
    destination_id && execution_id
      ? ["/transfers", destination_id, execution_id]
      : null,
    ([url, destination_id, execution_id]) =>
      getRequest(
        `${url}?destination_id=${destination_id}&execution_id=${execution_id}`
      )
  );
  const otherTransferRunsWithSameExecutionId = transferRunsWithSameExecutionId
    ?.filter((run) => run.transfer_run_id !== transferRun.transfer_run_id)
    .sort((a, b) => dayjs(b.created_at).diff(dayjs(a.created_at)));
  const hasOtherRuns = otherTransferRunsWithSameExecutionId?.length > 0;

  if (
    transferRunError ||
    transferError ||
    transferRunsWithSameExecutionIdError
  ) {
    return (
      <DashboardCard title="Transfer Details">
        <Alert severity="error">
          Error loading transfer details: {transferRunError.message}
        </Alert>
      </DashboardCard>
    );
  }

  if (
    transferRunLoading ||
    transferLoading ||
    transferRunsWithSameExecutionIdLoading
  ) {
    return (
      <Box sx={{ width: "100%" }}>
        <LinearProgress color="inherit" />
      </Box>
    );
  }

  if (!transferRun || !transfer) {
    return (
      <DashboardCard title="Transfer Details">
        <Alert severity="warning">Transfer not found</Alert>
      </DashboardCard>
    );
  }

  const getStatus = (status) => {
    if (status.toLowerCase().includes("success")) {
      return "Success ✅";
    }
    if (status.toLowerCase().includes("failed")) {
      return "Failed ❌";
    }
    if (status.toLowerCase().includes("running")) {
      return "Running ⏳";
    }
    return status;
  };

  const getDataTransferInterval = (transferMode) => {
    if (transferMode?.type === "FULL_REFRESH") {
      return `Full Refresh at ${dayjs(transferRun.created_at)
        .format("MMM D, h:mm:ss A z")
        .toString()}`;
    } else if (transferMode?.type === "INCREMENTAL") {
      return `${dayjs(transferMode?.start).format(
        "MMM D, h:mm:ss A z"
      )} - ${dayjs(transferMode?.end).format("MMM D, h:mm:ss A z")}`;
    }
    return "N/A";
  };
  const dataTransferInterval = getDataTransferInterval(
    transferRun?.meta?.arguments?.mode
  );

  const dataForTable = [
    ["Status", getStatus(transferRun.status)],
    [
      "Duration",
      dayjs
        .duration(dayjs(transferRun.modified_at).diff(transferRun.created_at))
        .humanize(),
    ],
    [
      "Transfer Run Start Time",
      dayjs(transferRun.created_at).format("MMM D, h:mm:ss A z").toString(),
    ],
    [
      "Transfer Run End Time",
      dayjs(transferRun.modified_at).format("MMM D, h:mm:ss A z").toString(),
    ],
    ["Data Transfer Interval", dataTransferInterval],
    ["Transfer Run ID", transferRun.transfer_run_id],
  ];

  return (
    <DashboardCard
      title={`Transfer Run: ${transferRun.transfer_run_id}`}
      topContent={
        <Button
          variant="contained"
          onClick={() => router.back()}
          sx={{ marginBottom: "24px" }}
          startIcon={<ChevronLeft />}
        >
          <Typography>Back</Typography>
        </Button>
      }
    >
      <Stack spacing={3}>
        <ListTable data={dataForTable} />

        {transferRun.output && (
          <Box>
            <Typography variant="h6" gutterBottom>
              Output
            </Typography>
            <Box
              sx={{
                backgroundColor: "grey.100",
                padding: 2,
                borderRadius: 1,
                fontFamily: "monospace",
                whiteSpace: "pre-wrap",
                overflow: "auto",
                maxHeight: "400px",
              }}
            >
              {JSON.stringify(transferRun.output, null, 2)}
            </Box>
          </Box>
        )}

        {transferRun.meta && (
          <Box>
            <Typography variant="h6" gutterBottom>
              Metadata
            </Typography>
            <Box
              sx={{
                backgroundColor: "grey.100",
                padding: 2,
                borderRadius: 1,
                fontFamily: "monospace",
                whiteSpace: "pre-wrap",
                overflow: "auto",
                maxHeight: "400px",
              }}
            >
              {JSON.stringify(transferRun.meta, null, 2)}
            </Box>
          </Box>
        )}

        {hasOtherRuns && otherTransferRunsWithSameExecutionId && (
          <Box>
            <Typography variant="h6" gutterBottom>
              Other Runs from this Execution Group
            </Typography>
            <Table>
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
                    <Typography variant="subtitle2" fontWeight={600}>
                      Transfer Run ID
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      Status
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="subtitle2" fontWeight={600}>
                      Transfer Started At
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ borderTopRightRadius: "5pt" }} />
                </TableRow>
              </TableHead>
              <TableBody>
                {otherTransferRunsWithSameExecutionId.map((run, idx) => (
                  <TableRow
                    key={idx}
                    hover={true}
                    sx={{
                      cursor: "pointer",
                    }}
                    onClick={() => {
                      router.push(`/transfers/${run.transfer_run_id}`);
                    }}
                  >
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {run.transfer_run_id}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {getStatus(run.status)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {dayjs(run.created_at).format("LLL z").toString()}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Box>
        )}
      </Stack>
    </DashboardCard>
  );
};

export default TransferDetails;
