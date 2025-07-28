"use client";
import {
  Alert,
  Typography,
  Button,
  Stack,
  Box,
  CircularProgress,
  LinearProgress,
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

dayjs.extend(LocalizedFormat);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);

const TransferDetails = () => {
  const params = useParams();
  const { id } = params;
  const router = useRouter();

  const {
    data: transferRun,
    error: transferRunError,
    isLoading: transferRunLoading,
  } = useSWR(`/transfers/${id}`, getRequest);

  if (transferRunError) {
    return (
      <DashboardCard title="Transfer Details">
        <Alert severity="error">
          Error loading transfer details: {transferRunError.message}
        </Alert>
      </DashboardCard>
    );
  }

  if (transferRunLoading) {
    return (
      <Box sx={{ width: "100%" }}>
        <LinearProgress color="inherit" />
      </Box>
    );
  }

  if (!transferRun) {
    return (
      <DashboardCard title="Transfer Details">
        <Alert severity="warning">Transfer not found</Alert>
      </DashboardCard>
    );
  }

  const dataForTable = [
    ["Transfer Run ID", transferRun.transfer_run_id],
    ["Transfer ID", transferRun.transfer_id],
    ["Status", transferRun.status],
    ["Created At", dayjs(transferRun.created_at).format("LLL z").toString()],
    ["Modified At", dayjs(transferRun.modified_at).format("LLL z").toString()],
  ];

  return (
    <DashboardCard
      title={`Transfer Run: ${transferRun.transfer_run_id}`}
      subtitle={`Status: ${transferRun.status}`}
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
      </Stack>
    </DashboardCard>
  );
};

export default TransferDetails;
