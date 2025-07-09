"use client";
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
  Stack,
} from "@mui/material";
import DashboardCard from "@/app/(DashboardLayout)//components/shared/DashboardCard";
import { IconDotsVertical, IconPlus, IconSettings } from "@tabler/icons-react";
import { useTheme } from "@mui/material/styles";
import _ from "lodash";

import useSWR from "swr";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";
import Link from "next/link";
import { useRouter } from "next/navigation";
import PageContainer from "@/app/(DashboardLayout)/components/container/PageContainer";
import Exports from "@/app/(DashboardLayout)/components/data-export/Exports";
import TableBodyWrapper from "@/app/(DashboardLayout)/components/shared/TableBodyWrapper";
import TablePlaceholderRow from "@/app/(DashboardLayout)/components/shared/TablePlaceholderRow";
import { getRequest } from "@/app/api/requests";
import { getVendorTypeDisplayText } from "@/utils/common";
import Image from "next/image";
import VendorLogo from "@/app/(DashboardLayout)/components/shared/VendorLogo";

dayjs.extend(relativeTime);

const getRecipientsById = (recipients) => {
  return recipients.reduce((acc, item) => {
    acc[item.recipient_id] = item;
    return acc;
  }, {});
};

const DestinationsTable = () => {
  const {
    data: destinations,
    error: destinationsError,
    isLoading: destinationsLoading,
  } = useSWR("/destinations", getRequest);
  const {
    data: recipients,
    error: recipientsError,
    isLoading: recipientsLoading,
  } = useSWR("/recipients", getRequest);
  const {
    data: models,
    error: modelsError,
    isLoading: isModelsLoading,
  } = useSWR("/models", getRequest);
  const {
    data: sources,
    error: sourcesError,
    isLoading: isSourcesLoading,
  } = useSWR("/sources", getRequest);
  const router = useRouter();
  const theme = useTheme();
  const isError =
    destinationsError || recipientsError || modelsError || sourcesError;
  const isLoading =
    destinationsLoading ||
    recipientsLoading ||
    isModelsLoading ||
    isSourcesLoading;
  const canAddDestination =
    _.isArray(sources) &&
    !_.isEmpty(sources) &&
    _.isArray(models) &&
    !_.isEmpty(models);
  const emptySources = _.isEmpty(sources);
  const emptyModels = _.isEmpty(models);
  const emptyRecipients = _.isEmpty(recipients);

  const renderPlaceholder = (emptySources, emptyModels, emptyRecipients) => {
    if (emptySources) {
      return (
        <TablePlaceholderRow
          numColumns={6}
          header={"No data sources connected"}
          message={
            "Connect a database source first — then you’ll be able to add models and set up export destinations."
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
      );
    } else if (emptyModels) {
      return (
        <TablePlaceholderRow
          numColumns={6}
          header={"No data models found"}
          message={
            "Before creating a destination, add the data models you want to sync."
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
      );
    } else if (emptyRecipients) {
      return (
        <TablePlaceholderRow
          numColumns={6}
          header={"Destinations need recipients"}
          message={
            "Before creating a destination, add recipients to define the customers receiving the data."
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
      );
    } else {
      return (
        <TablePlaceholderRow
          numColumns={6}
          header={"Let’s launch your first sync 🚀"}
          message={
            "Add a destination to start delivering data to your customers."
          }
          button={
            <Button
              variant="contained"
              href="/destinations/new"
              component={Link}
              startIcon={<IconPlus />}
            >
              <Typography>New Destination</Typography>
            </Button>
          }
        />
      );
    }
  };

  return (
    <PageContainer title="Destinations" description="Destinations for expore">
      <DashboardCard
        title="Destinations"
        action={
          <Button
            variant="contained"
            href="/destinations/new"
            component={Link}
            startIcon={<IconPlus />}
            disabled={!canAddDestination}
          >
            <Typography>New Destination</Typography>
          </Button>
        }
      >
        <Box sx={{ overflow: "auto", width: { xs: "280px", sm: "auto" } }}>
          <Table
            aria-label="destinations table"
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
                    Recipient
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Destination
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Enabled
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    Schedule
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
                isError={isError}
                isLoading={isLoading}
                numRows={6}
                numColumns={6}
                isEmpty={_.isEmpty(destinations)}
                emptyComponent={renderPlaceholder(
                  emptySources,
                  emptyModels,
                  emptyRecipients
                )}
              >
                {destinations &&
                  recipients &&
                  destinations.map((destination) => (
                    <TableRow
                      key={destination.destination_id}
                      onClick={() =>
                        router.push(
                          `/destinations/${destination.destination_id}`
                        )
                      }
                      hover={true}
                      sx={{ cursor: "pointer" }}
                    >
                      <TableCell>
                        <Typography variant="subtitle2" fontWeight={600}>
                          {destination.destination_name}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="subtitle2" fontWeight={600}>
                          {
                            getRecipientsById(recipients)[
                              destination.recipient_id
                            ].recipient_name
                          }
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Stack
                          direction="row"
                          spacing={0.5}
                          sx={{ alignItems: "center" }}
                        >
                          <VendorLogo vendor={destination.vendor_type} />
                          <Typography
                            noWrap
                            variant="subtitle2"
                            fontWeight={600}
                          >
                            {getVendorTypeDisplayText(destination.vendor_type)}
                          </Typography>
                        </Stack>
                      </TableCell>
                      <TableCell>
                        <Typography noWrap variant="subtitle2" fontWeight={600}>
                          {destination.is_enabled ? (
                            <Chip
                              variant="outlined"
                              label="Enabled"
                              color="success"
                              sx={{
                                bgcolor: "success.light",
                                color: "success.dark",
                                fontWeight: "bold",
                              }}
                            />
                          ) : (
                            <Chip
                              variant="outlined"
                              label="Disabled"
                              color="warning"
                              sx={{
                                bgcolor: "warning.light",
                                color: "text.secondary",
                                fontWeight: "bold",
                              }}
                            />
                          )}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography noWrap variant="subtitle2" fontWeight={600}>
                          {_.capitalize(
                            destination.schedule.frequency.toLowerCase()
                          )}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography noWrap variant="subtitle2" fontWeight={600}>
                          {dayjs(destination.created_at).fromNow()}
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
    </PageContainer>
  );
};

export default DestinationsTable;
