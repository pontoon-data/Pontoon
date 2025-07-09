import { Box, Drawer, Typography, Grid } from "@mui/material";
import Image from "next/image";
import SidebarItems from "./SidebarItems";

const SidebarLogo = () => {
  return (
    <Box>
      <Grid container spacing={2} sx={{ marginTop: "7px" }}>
        <Grid item xs={3}>
          <Image
            src="/images/pontoon-logo-dark.png"
            alt="Pontoon Logo"
            width={40}
            height={40}
            style={{
              marginLeft: "35px",
              borderRadius: 4,
            }}
          />
        </Grid>
        <Grid item xs={9}>
          <Typography
            variant="h2"
            sx={{ color: "black", marginLeft: "15px", marginTop: "1px" }}
          >
            Pontoon
          </Typography>
        </Grid>
      </Grid>
    </Box>
  );
};

const MSidebar = () => {
  const sidebarWidth = "250px";

  // Custom CSS for short scrollbar
  const scrollbarStyles = {
    "&::-webkit-scrollbar": {
      width: "7px",
    },
    "&::-webkit-scrollbar-thumb": {
      backgroundColor: "#eff2f7",
      borderRadius: "15px",
    },
  };

  return (
    <Box
      sx={{
        width: sidebarWidth,
        flexShrink: 0,
      }}
    >
      {/* ------------------------------------------- */}
      {/* Sidebar for desktop */}
      {/* ------------------------------------------- */}
      <Drawer
        anchor="left"
        open={true}
        variant="permanent"
        PaperProps={{
          sx: {
            boxSizing: "border-box",
            ...scrollbarStyles,
          },
        }}
      >
        {/* ------------------------------------------- */}
        {/* Sidebar Box */}
        {/* ------------------------------------------- */}
        <Box
          sx={{
            width: sidebarWidth,
            height: "100%",
          }}
        >
          {/* ------------------------------------------- */}
          {/* Logo */}
          {/* ------------------------------------------- */}
          {/* <Logo img="/images/logos/dark-logo.svg" /> */}
          <SidebarLogo />
          <Box>
            {/* ------------------------------------------- */}
            {/* Sidebar Items */}
            {/* ------------------------------------------- */}
            <SidebarItems />
          </Box>
          {/* </Sidebar> */}
        </Box>
      </Drawer>
    </Box>
  );
};

export default MSidebar;
