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
  Chip,
} from "@mui/material";
import DashboardCard from "@/app//components/shared/DashboardCard";
import { IconDotsVertical, IconPlus, IconSettings } from "@tabler/icons-react";
import { useTheme } from "@mui/material/styles";
import _ from "lodash";

import useSWR from "swr";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import Link from "next/link";
import { useRouter } from "next/navigation";
import TableBodyWrapper from "@/app/components/shared/TableBodyWrapper";
import TablePlaceholderRow from "@/app/components/shared/TablePlaceholderRow";
import { getRequest } from "@/app/api/requests";

dayjs.extend(relativeTime);

const RecipientsTable = () => {
  const {
    data: recipients,
    error,
    isLoading,
  } = useSWR("/recipients", getRequest);
  const router = useRouter();
  const theme = useTheme();

  return (
    <DashboardCard
      title="Recipients"
      action={
        <Button
          variant="contained"
          href="/recipients/new"
          component={Link}
          startIcon={<IconPlus />}
        >
          <Typography>New Recipient</Typography>
        </Button>
      }
    >
      <Box sx={{ overflow: "auto", width: { xs: "280px", sm: "auto" } }}>
        <Table
          aria-label="recipients table"
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
                  Tenant ID
                </Typography>
              </TableCell>
              <TableCell>
                <Typography variant="subtitle2" fontWeight={600}>
                  Created
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
              numColumns={3}
              isEmpty={_.isEmpty(recipients)}
              emptyComponent={
                <TablePlaceholderRow
                  numColumns={3}
                  header={"Start defining your data destinations"}
                  message={
                    "Recipients represent customers receiving data exports. Link them using a Tenant ID to ensure the right data gets to the right place."
                  }
                  button={
                    <Button
                      variant="contained"
                      href="/recipients/new"
                      component={Link}
                      startIcon={<IconPlus />}
                    >
                      <Typography>New Recipient</Typography>
                    </Button>
                  }
                />
              }
            >
              {recipients &&
                recipients.map((recipient) => (
                  <TableRow
                    key={recipient.recipient_id}
                    onClick={() =>
                      router.push(`/recipients/${recipient.recipient_id}`)
                    }
                    hover={true}
                    sx={{ cursor: "pointer" }}
                  >
                    <TableCell>
                      <Typography variant="subtitle2" fontWeight={600}>
                        {recipient.recipient_name}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={recipient.tenant_id}
                        variant="outlined" // Outlined border
                        color="success" // Themed background and border
                        sx={{
                          bgcolor: "success.light", // Custom background color
                          color: "success.dark", // Text color for contrast
                          fontWeight: "bold",
                        }}
                      />
                    </TableCell>
                    <TableCell>
                      <Typography noWrap variant="subtitle2" fontWeight={600}>
                        {dayjs(recipient.created_at).fromNow()}
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

export default RecipientsTable;
