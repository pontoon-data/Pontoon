import React from "react";
import {
  Typography,
  Select,
  Stack,
  FormHelperText,
  MenuItem,
} from "@mui/material";
import { useField } from "formik";

const FormSelect = ({
  titleText,
  helperText,
  placeholder,
  children,
  ...props
}) => {
  // useField() returns [formik.getFieldProps(), formik.getFieldMeta()]
  // which we can spread on <input>. We can use field meta to show an error
  // message if the field is invalid and it has been touched (i.e. visited)
  const [field, meta] = useField(props);
  return (
    <Stack>
      <Typography
        htmlFor={props.id || props.name}
        sx={{ fontWeight: 600, marginBottom: "4px" }}
      >
        {titleText}
      </Typography>
      <Select
        {...field}
        {...props}
        fullWidth
        displayEmpty
        renderValue={(selected) => {
          if (selected === "") return <>{placeholder}</>;

          const selectedChild = React.Children.toArray(children).find(
            (child) =>
              React.isValidElement(child) && child.props.value === selected
          );

          return selectedChild?.props.children ?? selected;
        }}
        error={meta.touched && meta.error}
      >
        {children}
      </Select>
      {meta.touched && meta.error ? (
        <FormHelperText error>{meta.error}</FormHelperText>
      ) : null}
      <FormHelperText>{helperText}</FormHelperText>
    </Stack>
  );
};

export default FormSelect;
