package me.yswnthdev.rtdps.service;

import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.entity.Customer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.StringUtils;

@Slf4j
public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

    @Override
    public Customer process(Customer customer) {
        // Trim whitespace from all string fields
        customer.setFirstName(trimOrNull(customer.getFirstName()));
        customer.setLastName(trimOrNull(customer.getLastName()));
        customer.setEmail(trimOrNull(customer.getEmail()));
        customer.setGender(trimOrNull(customer.getGender()));
        customer.setContactNo(trimOrNull(customer.getContactNo()));
        customer.setCountry(trimOrNull(customer.getCountry()));
        customer.setDob(trimOrNull(customer.getDob()));

        // Normalize email to lowercase
        if (customer.getEmail() != null) {
            customer.setEmail(customer.getEmail().toLowerCase());
        }

        // Capitalize first and last names
        customer.setFirstName(capitalize(customer.getFirstName()));
        customer.setLastName(capitalize(customer.getLastName()));

        // Validate email format — filter out invalid records
        if (customer.getEmail() == null || !customer.getEmail().contains("@")) {
            log.warn("[CustomerProcessor] Skipping customer id={} — invalid email: '{}'",
                    customer.getId(), customer.getEmail());
            return null; // returning null filters the item out
        }

        return customer;
    }

    private String trimOrNull(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        return value.trim();
    }

    private String capitalize(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        return value.substring(0, 1).toUpperCase() + value.substring(1).toLowerCase();
    }
}