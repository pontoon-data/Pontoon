"use client";
import PageContainer from "@/app/(DashboardLayout)/components/container/PageContainer";
import SourceTable from "@/app/(DashboardLayout)/sources/SourceTable";

const Sources = () => {
  return (
    <PageContainer title="Sources" description="Data Sources">
      <SourceTable />
    </PageContainer>
  );
};

export default Sources;
