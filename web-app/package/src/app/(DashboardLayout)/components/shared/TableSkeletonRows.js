import { TableCell, TableRow, Skeleton } from "@mui/material";

const SkeletonRow = ({ columns }) => (
  <TableRow>
    {Array.from({ length: columns }).map((_, index) => (
      <TableCell key={index}>
        <Skeleton variant="text" width="80%" height={24} />
      </TableCell>
    ))}
  </TableRow>
);

const TableSkeletonRows = ({ numRows, numColumns }) =>
  Array.from({ length: numRows }).map((_, index) => (
    <SkeletonRow key={index} columns={numColumns} />
  ));

export default TableSkeletonRows;
