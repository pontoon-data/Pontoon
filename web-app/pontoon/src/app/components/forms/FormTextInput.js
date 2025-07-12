import { Typography, Box, TextField, FormHelperText } from "@mui/material";
import { useField } from "formik";

const FormTextInput = ({ label, helperText, ...props }) => {
  const [field, meta] = useField(props);
  return (
    <Box>
      <Typography
        htmlFor={props.id || props.name}
        sx={{ fontWeight: 600, marginBottom: "4px" }}
      >
        {label}
      </Typography>
      <TextField
        {...field}
        {...props}
        fullWidth
        variant="outlined"
        error={meta.touched && meta.error}
        helperText={meta.touched && meta.error ? meta.error : ""}
      />
      <FormHelperText>{helperText}</FormHelperText>
    </Box>
  );
};

export default FormTextInput;
