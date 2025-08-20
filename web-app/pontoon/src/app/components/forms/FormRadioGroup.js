import React from "react";
import {
  Typography,
  FormControl,
  FormControlLabel,
  Radio,
  RadioGroup,
  FormHelperText,
  Stack,
} from "@mui/material";
import { useField } from "formik";

const FormRadioGroup = ({ titleText, helperText, options = [], ...props }) => {
  const [field, meta] = useField(props);
  return (
    <Stack>
      <Typography
        htmlFor={props.id || props.name}
        sx={{ fontWeight: 600, marginBottom: "4px" }}
      >
        {titleText}
      </Typography>
      <FormControl error={meta.touched && meta.error} fullWidth>
        <RadioGroup {...field} {...props} row>
          {options.map((option) => (
            <FormControlLabel
              key={option.value}
              value={option.value}
              control={<Radio />}
              label={option.label}
            />
          ))}
        </RadioGroup>
      </FormControl>
      {meta.touched && meta.error ? (
        <FormHelperText error>{meta.error}</FormHelperText>
      ) : null}
      <FormHelperText>{helperText}</FormHelperText>
    </Stack>
  );
};

export default FormRadioGroup;
