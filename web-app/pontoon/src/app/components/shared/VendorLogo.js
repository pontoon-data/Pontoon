import { Box } from "@mui/material";
import Image from "next/image";

const VendorLogo = ({ vendor }) => {
  const details = {
    snowflake: {
      name: "Snowflake",
      image: "/images/vendor-logos/snowflake-logo-white.svg",
      backgroundColor: "#29B5E8",
    },
    bigquery: {
      name: "BigQuery",
      image: "/images/vendor-logos/bigquery-logo.svg",
      backgroundColor: "grey.300",
    },
    redshift: {
      name: "Redshift",
      image: "/images/vendor-logos/redshift-logo.svg",
      backgroundColor: "grey.300",
    },
    postgresql: {
      name: "Postgres",
      image: "/images/vendor-logos/postgres-logo.svg",
      backgroundColor: "grey.300",
    },

    // Use snowflake image for dev vendors
    memory: {
      image: "/images/vendor-logos/snowflake-logo.svg",
      backgroundColor: "#29B5E8",
    },
    console: {
      image: "/images/vendor-logos/snowflake-logo.svg",
      backgroundColor: "#29B5E8",
    },
  };
  const vendorDetails = details[vendor];
  return (
    <Box
      sx={{
        backgroundColor: `${vendorDetails.backgroundColor}`,
        borderRadius: "50%",
        padding: 0.7,
        position: "relative",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        border: "1px solid",
        borderColor: "grey.400",
      }}
    >
      <Image
        src={vendorDetails.image}
        alt={`${vendorDetails.name} logo`}
        width={17}
        height={17}
      />
    </Box>
  );
};

export default VendorLogo;
