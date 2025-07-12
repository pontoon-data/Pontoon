"use client";
import PageContainer from "@/app/components/container/PageContainer";
import AddModelForm from "@/app/models/new/AddModelForm";

const AddModels = () => {
  return (
    <PageContainer title="Sources" description="Data Sources">
      <AddModelForm />
    </PageContainer>
  );
};

export default AddModels;
