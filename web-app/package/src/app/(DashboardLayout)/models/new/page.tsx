"use client";
import PageContainer from "@/app/(DashboardLayout)/components/container/PageContainer";
import AddModelForm from "@/app/(DashboardLayout)/models/new/AddModelForm";

const AddModels = () => {
  return (
    <PageContainer title="Sources" description="Data Sources">
      <AddModelForm />
    </PageContainer>
  );
};

export default AddModels;
