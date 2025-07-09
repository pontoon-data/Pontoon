"use client";
import PageContainer from "@/app/(DashboardLayout)/components/container/PageContainer";
import AddSourceForm from "@/app/(DashboardLayout)/sources/new/AddSourceForm";

const AddSources = () => {
  return (
    <PageContainer title="Sources" description="Data Sources">
      <AddSourceForm />
    </PageContainer>
  );
};

export default AddSources;
