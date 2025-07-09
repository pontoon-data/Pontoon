"use client";
import PageContainer from "@/app/(DashboardLayout)/components/container/PageContainer";
import RecipientsTable from "@/app/(DashboardLayout)/recipients/RecipientsTable";

const RecipientsHome = () => {
  return (
    <PageContainer title="Recipients" description="Add recipients for export">
      <RecipientsTable />
    </PageContainer>
  );
};

export default RecipientsHome;
