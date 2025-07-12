"use client";
import {
  Typography,
  Box,
  FormControl,
  FormHelperText,
  Button,
  Grid,
  Stack,
} from "@mui/material";
import { Form, Formik } from "formik";
import * as Yup from "yup";
import useSWRMutation from "swr/mutation";
import Link from "next/link";
import { ChevronLeft } from "@mui/icons-material";
import { useRouter } from "next/navigation";

import FormTextInput from "@/app/components/forms/FormTextInput";
import PageContainer from "@/app/components/container/PageContainer";
import { postRequest } from "@/app/api/requests";

const AddRecipients = () => {
  const { trigger } = useSWRMutation("/recipients", postRequest);
  const router = useRouter();
  return (
    <PageContainer title="Add Recipients" description="Add Recipients">
      <Box>
        <Button
          variant="contained"
          href="/recipients"
          component={Link}
          startIcon={<ChevronLeft />}
        >
          <Typography>Back</Typography>
        </Button>
        <Formik
          initialValues={{
            recipient_name: "",
            tenant_id: "",
          }}
          validationSchema={Yup.object({
            recipient_name: Yup.string()
              .max(64, "Must be 64 characters or less")
              .required("Required"),
            tenant_id: Yup.string().required("Required"),
          })}
          onSubmit={(values, { setSubmitting }) => {
            trigger(values);
            router.push("/recipients");
            setSubmitting(false);
          }}
        >
          {({ values }) => (
            <Form>
              <Stack maxWidth={"sm"} spacing={3}>
                <Typography variant="h2" sx={{ paddingTop: "20px" }}>
                  Create Recipient
                </Typography>

                <FormTextInput
                  label="Name"
                  name="recipient_name"
                  helperText="Descriptive name only used internally for reference."
                />

                <FormControl
                  sx={{
                    bgcolor: "warning.light",
                    borderRadius: 2,
                    padding: 1,
                  }}
                >
                  <FormTextInput label="⚠️ Tenant ID" name="tenant_id" />

                  <FormHelperText sx={{ ml: 0 }}>
                    This value will be used to determine which model rows will
                    be synced to this recipient.
                  </FormHelperText>
                </FormControl>

                <Button
                  type="submit"
                  variant="contained"
                  sx={{ width: "fit-content" }}
                >
                  Create
                </Button>
              </Stack>
            </Form>
          )}
        </Formik>
      </Box>
    </PageContainer>
  );
};

export default AddRecipients;
