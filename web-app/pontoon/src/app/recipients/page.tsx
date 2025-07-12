"use client";
import PageContainer from "@/app/components/container/PageContainer";
import RecipientsTable from "@/app/recipients/RecipientsTable";

const RecipientsHome = () => {
  return (
    <PageContainer title="Recipients" description="Add recipients for export">
      <RecipientsTable />
    </PageContainer>
  );
};

export default RecipientsHome;
