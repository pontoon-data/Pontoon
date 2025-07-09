"use client";
import PageContainer from "@/app/(DashboardLayout)/components/container/PageContainer";
import ModelsTable from "@/app/(DashboardLayout)/models/ModelsTable";

const Models = () => {
  return (
    <PageContainer title="Models" description="Create models for export">
      <ModelsTable />
    </PageContainer>
  );
};

export default Models;
