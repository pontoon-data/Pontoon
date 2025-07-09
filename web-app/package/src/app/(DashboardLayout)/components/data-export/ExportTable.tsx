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
} from "@mui/material";
import DashboardCard from "@/app/(DashboardLayout)//components/shared/DashboardCard";
import { IconDotsVertical, IconPlus, IconSettings } from "@tabler/icons-react";
import FormDialog from "./CreateExportDialog";

const products = [
  {
    id: "1",
    name: "campaigns-export",
    post: "Web Designer",
    pname: "Elite Admin",
    priority: "Low",
    pbg: "success.main",
    budget: "3.9",
    destination: "Amazon S3",
    status: "OK",
    scheduledAt: "Daily @ 3:00 AM PST",
    createdAt: "Dec 2, 2024 1:04 PM PST",
    createdBy: "test-user",
    lastSuccessfulSync: "Dec 5, 2024 3:00 AM PST",
  },
  {
    id: "2",
    name: "leads-export",
    post: "Project Manager",
    pname: "Real Homes WP Theme",
    priority: "Medium",
    pbg: "success.main",
    budget: "24.5",
    destination: "Amazon S3",
    status: "OK",
    scheduledAt: "Daily @ 3:00 AM PST",
    createdAt: "Dec 2, 2024 1:04 PM PST",
    createdBy: "test-user",
    lastSuccessfulSync: "Dec 5, 2024 3:00 AM PST",
  },
  {
    id: "3",
    name: "prospects-export",
    post: "Project Manager",
    pname: "MedicalPro WP Theme",
    priority: "High",
    pbg: "success.main",
    budget: "12.8",
    destination: "Redshift",
    status: "OK",
    scheduledAt: "Daily @ 2:00 AM PST",
    createdAt: "Dec 4, 2024 2:46 PM PST",
    createdBy: "data-team",
    lastSuccessfulSync: "Dec 5, 2024 3:00 AM PST",
  },
  {
    id: "4",
    name: "customers-export",
    post: "Frontend Engineer",
    pname: "Hosting Press HTML",
    priority: "Critical",
    pbg: "warning.main",
    budget: "2.4",
    destination: "Redshift",
    status: "Paused",
    scheduledAt: "Daily @ 2:00 AM PST",
    createdAt: "Dec 4, 2024 2:46 PM PST",
    createdBy: "data-team",
    lastSuccessfulSync: "Dec 5, 2024 3:00 AM PST",
  },
];

const ExportTable = () => {
  return (
    <DashboardCard title="Exports" action={<FormDialog />}>
      <Box sx={{ overflow: "auto", width: { xs: "280px", sm: "auto" } }}>
        <Table
          aria-label="simple table"
          sx={{
            whiteSpace: "nowrap",
            mt: 2,
          }}
        >
          <TableHead>
            <TableRow>
              <TableCell>
                <Typography variant="subtitle2" fontWeight={600}>
                  Name
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
              <TableCell>
                <Typography variant="subtitle2" fontWeight={600}>
                  Created By
                </Typography>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {products.map((product) => (
              <TableRow key={product.name}>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    {product.name}
                  </Typography>
                </TableCell>
                <TableCell>
                  <Typography variant="subtitle2" fontWeight={600}>
                    {product.destination}
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
                  <Typography variant="subtitle2" fontWeight={600}>
                    {product.createdBy}
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
};

export default ExportTable;
