"use client";
import { Typography, Button, Stack } from "@mui/material";
import useSWR from "swr";
import DashboardCard from "@/app//components/shared/DashboardCard";
import { ChevronLeft } from "@mui/icons-material";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import Link from "next/link";
import ListTable from "@/app/components/shared/ListTable";
import useSWRMutation from "swr/mutation";
import { useRouter } from "next/navigation";

import dayjs from "dayjs";
import LocalizedFormat from "dayjs/plugin/localizedFormat";
import { getRequest, deleteRequest } from "@/app/api/requests";

dayjs.extend(LocalizedFormat);

const getDataForTable = (data) => {
  const d = data;
  switch (d.vendor_type) {
    case "snowflake":
      return [
        ["Vendor", "Snowflake"],
        ["Account", d.connection_info.account],
        ["Warehouse", d.connection_info.warehouse],
        ["Created", dayjs(d.created_at).format("LLL").toString()],
        ["Updated", dayjs(d.modified_at).format("LLL").toString()],
      ];
    case "bigquery":
      return [
        ["Vendor", "BigQuery"],
        ["Project ID", d.connection_info.project_id],
        ["Dataset", d.connection_info.dataset],
        ["Created At", dayjs(d.created_at).format("LLL").toString()],
        ["Updated", dayjs(d.modified_at).format("LLL").toString()],
      ];
    case "redshift":
      return [
        ["Vendor", "Redshift"],
        ["Host Name", d.connection_info.host],
        ["Port", d.connection_info.port],
        ["Database", d.connection_info.database],
        ["Username", d.connection_info.user],
        ["Created At", dayjs(d.created_at).format("LLL").toString()],
        ["Updated", dayjs(d.modified_at).format("LLL").toString()],
      ];
    case "postgresql":
      return [
        ["Vendor", "Postgres"],
        ["Host Name", d.connection_info.host],
        ["Port", d.connection_info.port],
        ["Database", d.connection_info.database],
        ["Username", d.connection_info.user],
        ["Created At", dayjs(d.created_at).format("LLL").toString()],
        ["Updated", dayjs(d.modified_at).format("LLL").toString()],
      ];
    case "memory":
      return [
        ["Vendor", "Memory"],
        ["Created At", dayjs(d.created_at).format("LLL").toString()],
        ["Updated", dayjs(d.modified_at).format("LLL").toString()],
      ];
    default:
      console.log("Error reading data for details");
  }
};

const SourceDetails = ({ params }) => {
  const { id } = params;
  const {
    data: source,
    error: isError,
    isLoading,
  } = useSWR(`/sources/${id}`, getRequest);
  const { trigger: triggerDeleteSource } = useSWRMutation(
    `/sources/${id}`,
    deleteRequest
  );

  const router = useRouter();

  if (isError) {
    return <Typography>Error with API</Typography>;
  }
  if (isLoading) {
    return <Typography>Loading screen!</Typography>;
  }

  const dataForTable = getDataForTable(source);

  return (
    <DashboardCard
      title={source.source_name}
      topContent={
        <Button
          variant="contained"
          href="/sources"
          component={Link}
          sx={{ marginBottom: "24px" }}
          startIcon={<ChevronLeft />}
        >
          <Typography>Back</Typography>
        </Button>
      }
    >
      <ListTable title={"Details"} data={dataForTable} />
      <Stack direction="row" spacing={3} marginTop="20px">
        <Button
          variant="contained"
          startIcon={<DeleteIcon />}
          color="error"
          onClick={() => {
            triggerDeleteSource();
            router.push("/sources");
          }}
        >
          Delete
        </Button>
      </Stack>
    </DashboardCard>
  );
};

export default SourceDetails;
