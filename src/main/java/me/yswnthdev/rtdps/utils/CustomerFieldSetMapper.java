package me.yswnthdev.rtdps.utils;

import me.yswnthdev.rtdps.entity.Customer;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.util.StringUtils;

public class CustomerFieldSetMapper implements FieldSetMapper<Customer> {

    private final String trackerId;

    public CustomerFieldSetMapper(String trackerId) {
        this.trackerId = trackerId;
    }

    @Override
    public Customer mapFieldSet(FieldSet fieldSet) {
        String[] columns = {
                "id", "firstName", "lastName", "email",
                "gender", "contactNo", "country", "dob"
        };

        StringBuilder missing = new StringBuilder();

        for (String col : columns) {
            String value = fieldSet.readString(col);

            // if null or empty or blank
            if (!StringUtils.hasText(value)) {
                missing.append(col).append(", ");
            }
        }

        // If missing, log and throw so Spring Batch's skip mechanism handles it
        // (returning null here would terminate reading — Spring Batch treats null as end-of-stream)
        if (!missing.isEmpty()) {
            String row = String.join(",", fieldSet.getValues());
            BatchErrorLogger.log(trackerId,
                    "missing=" + missing.substring(0, missing.length() - 2) +
                    " | row=" + row);
            throw new IllegalArgumentException(
                    "Missing required columns: " + missing.substring(0, missing.length() - 2));
        }

        // Map fields manually
        Customer c = new Customer();
        c.setId(fieldSet.readInt("id"));
        c.setFirstName(fieldSet.readString("firstName"));
        c.setLastName(fieldSet.readString("lastName"));
        c.setEmail(fieldSet.readString("email"));
        c.setGender(fieldSet.readString("gender"));
        c.setContactNo(fieldSet.readString("contactNo"));
        c.setCountry(fieldSet.readString("country"));
        c.setDob(fieldSet.readString("dob"));

        return c;
    }
}

