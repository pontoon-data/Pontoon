import TableSkeletonRows from "@/app/(DashboardLayout)/components/shared/TableSkeletonRows";
import TableErrorRow from "@/app/(DashboardLayout)/components/shared/TableErrorRow";

const TableBodyWrapper = ({
  children,
  isError,
  isLoading,
  numRows,
  numColumns,
  isEmpty,
  emptyComponent,
}) => {
  if (isLoading && !isError) {
    return <TableSkeletonRows numRows={numRows} numColumns={numColumns} />;
  } else if (isError) {
    return <TableErrorRow colSpan={numColumns} />;
  } else if (isEmpty && !isLoading && !isError) {
    return <>{emptyComponent}</>;
  } else if (!isLoading && !isError) {
    return <>{children}</>;
  }
};

export default TableBodyWrapper;
