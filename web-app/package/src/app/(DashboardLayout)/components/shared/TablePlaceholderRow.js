import { TableCell, TableRow, Typography, Box } from "@mui/material";

const TablePlaceholderRow = ({ numColumns, header, message, button }) => (
  <TableRow>
    <TableCell colSpan={numColumns} align="center">
      <Box sx={{ paddingBottom: 3 }}>
        <Typography
          variant="h5"
          fontWeight={700}
          paddingTop={6}
          paddingBottom={1}
          whiteSpace="pre-line"
        >
          {header}
        </Typography>
        <Typography
          variant="subtitle2"
          fontWeight={600}
          paddingBottom={2}
          whiteSpace="pre-line"
        >
          {message}
        </Typography>
        <>{button}</>
      </Box>
    </TableCell>
  </TableRow>
);

export default TablePlaceholderRow;
