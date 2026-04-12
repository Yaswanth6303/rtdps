package me.yswnthdev.rtdps.service;

import me.yswnthdev.rtdps.entity.Customer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CustomerProcessorTest {

    private CustomerProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new CustomerProcessor();
    }

    @Test
    @DisplayName("Should trim whitespace from all fields")
    void shouldTrimWhitespace() {
        Customer customer = new Customer(1, "  John  ", "  Doe  ", "  john@example.com  ",
                "  Male  ", "  1234567890  ", "  India  ", "  01-01-1990  ");

        Customer result = processor.process(customer);

        assertThat(result).isNotNull();
        assertThat(result.getFirstName()).isEqualTo("John");
        assertThat(result.getLastName()).isEqualTo("Doe");
        assertThat(result.getEmail()).isEqualTo("john@example.com");
        assertThat(result.getGender()).isEqualTo("Male");
        assertThat(result.getContactNo()).isEqualTo("1234567890");
        assertThat(result.getCountry()).isEqualTo("India");
        assertThat(result.getDob()).isEqualTo("01-01-1990");
    }

    @Test
    @DisplayName("Should normalize email to lowercase")
    void shouldNormalizeEmail() {
        Customer customer = new Customer(1, "John", "Doe", "JOHN@EXAMPLE.COM",
                "Male", "1234567890", "India", "01-01-1990");

        Customer result = processor.process(customer);

        assertThat(result).isNotNull();
        assertThat(result.getEmail()).isEqualTo("john@example.com");
    }

    @Test
    @DisplayName("Should capitalize first and last names")
    void shouldCapitalizeNames() {
        Customer customer = new Customer(1, "jOHN", "dOE", "john@example.com",
                "Male", "1234567890", "India", "01-01-1990");

        Customer result = processor.process(customer);

        assertThat(result).isNotNull();
        assertThat(result.getFirstName()).isEqualTo("John");
        assertThat(result.getLastName()).isEqualTo("Doe");
    }

    @Test
    @DisplayName("Should filter out customer with null email")
    void shouldFilterNullEmail() {
        Customer customer = new Customer(1, "John", "Doe", null,
                "Male", "1234567890", "India", "01-01-1990");

        Customer result = processor.process(customer);

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Should filter out customer with invalid email (no @)")
    void shouldFilterInvalidEmail() {
        Customer customer = new Customer(1, "John", "Doe", "notanemail",
                "Male", "1234567890", "India", "01-01-1990");

        Customer result = processor.process(customer);

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Should filter out customer with empty email")
    void shouldFilterEmptyEmail() {
        Customer customer = new Customer(1, "John", "Doe", "   ",
                "Male", "1234567890", "India", "01-01-1990");

        Customer result = processor.process(customer);

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Should pass through valid customer")
    void shouldPassValidCustomer() {
        Customer customer = new Customer(1, "John", "Doe", "john@example.com",
                "Male", "1234567890", "India", "01-01-1990");

        Customer result = processor.process(customer);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(1);
        assertThat(result.getFirstName()).isEqualTo("John");
        assertThat(result.getLastName()).isEqualTo("Doe");
        assertThat(result.getEmail()).isEqualTo("john@example.com");
    }
}
