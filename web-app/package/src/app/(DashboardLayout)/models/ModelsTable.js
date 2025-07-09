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
} from "@mui/material";
import DashboardCard from "@/app/(DashboardLayout)//components/shared/DashboardCard";
import { IconDotsVertical, IconPlus, IconSettings } from "@tabler/icons-react";
import { useTheme } from "@mui/material/styles";
import useSWR from "swr";
import _ from "lodash";

import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import Link from "next/link";
import { useRouter } from "next/navigation";
import TableBodyWrapper from "@/app/(DashboardLayout)/components/shared/TableBodyWrapper";
import TablePlaceholderRow from "@/app/(DashboardLayout)/components/shared/TablePlaceholderRow";
import { getRequest } from "@/app/api/requests";

dayjs.extend(relativeTime);

/*

Example models:

table: "exports.campaigns",
description: "Tracks the performance and metrics of marketing campaigns"

table: "exports.leads",
description: "Expressed interest in a product but are not fully qualified as potential customers"

table: "exports.prospects",
description: "Qualified as potential customers with a higher likelihood of conversion"

*/

const ModelsTable = () => {
  const { data, error, isLoading } = useSWR("/models", getRequest);
  const {
    data: sources,
    error: sourcesError,
    isLoading: isSourcesLoading,
  } = useSWR("/sources", getRequest);
  const router = useRouter();
  const theme = useTheme();
  return (
    <DashboardCard
      title="Models"
      action={
        <Button
          variant="contained"
          href="/models/new"
          component={Link}
          startIcon={<IconPlus />}
          disabled={isSourcesLoading || sourcesError || _.isEmpty(sources)}
        >
          <Typography>New Model</Typography>
        </Button>
      }
    >
      <Box sx={{ overflow: "auto", width: { xs: "280px", sm: "auto" } }}>
        <Table
          aria-label="simple table"
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
                <Typography variant="subtitle2" fontWeight={600}>
                  Name
                </Typography>
              </TableCell>
              <TableCell>
                <Typography variant="subtitle2" fontWeight={600}>
                  Table
                </Typography>
              </TableCell>
              <TableCell>
                <Typography variant="subtitle2" fontWeight={600}>
                  Description
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
              isError={error}
              isLoading={isLoading}
              numRows={4}
              numColumns={4}
              isEmpty={_.isEmpty(data)}
              emptyComponent={
                _.isEmpty(sources) ? (
                  <TablePlaceholderRow
                    numColumns={4}
                    header={"No data sources connected"}
                    message={
                      "Connect a database source first — then you’ll be able to add models for export."
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
                ) : (
                  <TablePlaceholderRow
                    numColumns={4}
                    header={"Ready to load your models?"}
                    message={
                      "Add data models from your connected sources to make them available for export."
                    }
                    button={
                      <Button
                        variant="contained"
                        href="/models/new"
                        component={Link}
                        startIcon={<IconPlus />}
                      >
                        <Typography>New Model</Typography>
                      </Button>
                    }
                  />
                )
              }
            >
              {data &&
                data.map((model) => (
                  <TableRow
                    key={model.model_id}
                    onClick={() => router.push(`/models/${model.model_id}`)}
                    hover={true}
                    sx={{ cursor: "pointer" }}
                  >
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {model.model_name}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {model.schema_name}.{model.table_name}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography noWrap variant="subtitle2" fontWeight={600}>
                        {model.model_description}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {dayjs(model.created_at).fromNow()}
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

export default ModelsTable;
