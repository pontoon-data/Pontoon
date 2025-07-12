"use client";
import PageContainer from "@/app/components/container/PageContainer";
import AddSourceForm from "@/app/sources/new/AddSourceForm";

const AddSources = () => {
  return (
    <PageContainer title="Sources" description="Data Sources">
      <AddSourceForm />
    </PageContainer>
  );
};

export default AddSources;
