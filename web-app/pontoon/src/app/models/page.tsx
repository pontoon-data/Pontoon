"use client";
import PageContainer from "@/app/components/container/PageContainer";
import ModelsTable from "@/app/models/ModelsTable";

const Models = () => {
  return (
    <PageContainer title="Models" description="Create models for export">
      <ModelsTable />
    </PageContainer>
  );
};

export default Models;
