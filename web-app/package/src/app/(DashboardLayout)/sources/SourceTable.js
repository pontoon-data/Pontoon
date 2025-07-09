import {
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Button,
  IconButton,
  Tooltip,
  Skeleton,
} from "@mui/material";
import DashboardCard from "@/app/(DashboardLayout)//components/shared/DashboardCard";
import { IconDotsVertical, IconPlus } from "@tabler/icons-react";
import { useTheme } from "@mui/material/styles";
import { useRouter } from "next/navigation";
import { getVendorTypeDisplayText } from "@/utils/common";

import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import Link from "next/link";
import _ from "lodash";

import TableBodyWrapper from "@/app/(DashboardLayout)/components/shared/TableBodyWrapper";
import TablePlaceholderRow from "@/app/(DashboardLayout)/components/shared/TablePlaceholderRow";
import { getRequest } from "@/app/api/requests";
import useSWR from "swr";

dayjs.extend(relativeTime);

const SourceTable = () => {
  const {
    data: sources,
    error: isError,
    isLoading,
  } = useSWR("/sources", getRequest);
  const filteredSources = sources?.filter((s) => s.state === "CREATED");

  const router = useRouter();
  const theme = useTheme();

  return (
    <DashboardCard
      title="Sources"
      action={
        <Button
          variant="contained"
          href="/sources/new"
          component={Link}
          startIcon={<IconPlus />}
        >
          <Typography>New Source</Typography>
        </Button>
      }
    >
      <Box sx={{ overflow: "auto", width: { xs: "280px", sm: "auto" } }}>
        <Table
          aria-label="simple table"
          sx={{
            whiteSpace: "nowrap",
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
                <Typography variant="subtitle2" fontWeight={600}>
                  Name
                </Typography>
              </TableCell>
              <TableCell>
                <Typography variant="subtitle2" fontWeight={600}>
                  Vendor
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
            <TableBodyWrapper
              isError={isError}
              isLoading={isLoading}
              numRows={4}
              numColumns={3}
              isEmpty={_.isEmpty(filteredSources)}
              emptyComponent={
                <TablePlaceholderRow
                  numColumns={3}
                  header={"Let’s connect your data"}
                  message={
                    "This table will list all your connected database sources. Add one now to get started."
                  }
                  button={
                    <Button
                      variant="contained"
                      href="/sources/new"
                      component={Link}
                      startIcon={<IconPlus />}
                    >
                      <Typography>New Source</Typography>
                    </Button>
                  }
                />
              }
            >
              {filteredSources &&
                filteredSources.map((source) => (
                  <TableRow
                    key={source.source_id}
                    onClick={() => router.push(`/sources/${source.source_id}`)}
                    hover={true}
                    sx={{ cursor: "pointer" }}
                  >
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {source.source_name}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {getVendorTypeDisplayText(source.vendor_type)}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {dayjs(source.created_at).fromNow()}
                      </Typography>
                    </TableCell>
                    <TableCell align="right">
                      <Tooltip title="Options" placement="right">
                        <IconButton>
                          <IconDotsVertical />
                        </IconButton>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
            </TableBodyWrapper>
          </TableBody>
        </Table>
      </Box>
    </DashboardCard>
  );
};

export default SourceTable;
