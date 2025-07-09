import { Container, Box } from "@mui/material";
import React from "react";
import Sidebar from "@/app/(DashboardLayout)/layout/sidebar/Sidebar";

interface Props {
  children: React.ReactNode;
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <Box
      className="mainwrapper"
      sx={{
        display: "flex",
        minHeight: "100vh",
        width: "100%",
      }}
    >
      {/* ------------------------------------------- */}
      {/* Sidebar */}
      {/* ------------------------------------------- */}
      <Sidebar />
      {/* ------------------------------------------- */}
      {/* Main Wrapper */}
      {/* ------------------------------------------- */}
      <Box
        className="page-wrapper"
        sx={{
          display: "flex",
          flexGrow: 1,
          paddingBottom: "60px",
          flexDirection: "column",
          zIndex: 1,
          backgroundColor: "transparent",
        }}
      >
        {/* ------------------------------------------- */}
        {/* PageContent */}
        {/* ------------------------------------------- */}
        <Container
          sx={{
            paddingTop: "20px",
            maxWidth: "1536px",
          }}
          maxWidth="xl"
        >
          {/* ------------------------------------------- */}
          {/* Page Route */}
          {/* ------------------------------------------- */}
          <Box sx={{ minHeight: "calc(100vh - 170px)" }}>{children}</Box>
          {/* ------------------------------------------- */}
          {/* End Page */}
          {/* ------------------------------------------- */}
        </Container>
      </Box>
    </Box>
  );
}
