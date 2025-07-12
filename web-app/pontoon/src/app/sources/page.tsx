"use client";
import PageContainer from "@/app/components/container/PageContainer";
import SourceTable from "@/app/sources/SourceTable";

const Sources = () => {
  return (
    <PageContainer title="Sources" description="Data Sources">
      <SourceTable />
    </PageContainer>
  );
};

export default Sources;
