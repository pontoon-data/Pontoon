import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
} from "@mui/material";

const TableErrorRow = ({ colSpan, message }) => (
  <TableRow>
    <TableCell colSpan={colSpan} align="center">
      <Typography
        variant="subtitle2"
        fontWeight={600}
        paddingTop={8}
        paddingBottom={8}
        whiteSpace="pre-line"
      >
        Oops, something went wrong.{"\n"}Try refreshing the page.
      </Typography>
    </TableCell>
  </TableRow>
);

export default TableErrorRow;
