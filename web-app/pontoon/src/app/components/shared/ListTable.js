import {
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
} from "@mui/material";

const ListTable = ({ title, data }) => {
  return (
    <Table>
      {title && (
        <TableHead>
          <TableRow
            sx={{
              borderBottomColor: "#F2F6FA",
              borderBottomStyle: "solid",
              borderBottomWidth: "2px",
            }}
          >
            <TableCell sx={{ paddingLeft: 0 }}>
              <Typography variant="h3">{title}</Typography>
            </TableCell>
          </TableRow>
        </TableHead>
      )}
      <TableBody>
        {data &&
          data.map((list, index) => (
            <TableRow
              key={index}
              sx={{
                borderBottomColor: "#F2F6FA",
                borderBottomStyle: "solid",
                borderBottomWidth: "2px",
              }}
            >
              <TableCell sx={{ paddingLeft: "2px" }}>
                <Typography fontWeight={600}>{list[0]}</Typography>
              </TableCell>
              <TableCell>
                <Typography>{String(list[1])}</Typography>
              </TableCell>
            </TableRow>
          ))}
      </TableBody>
    </Table>
  );
};

export default ListTable;
