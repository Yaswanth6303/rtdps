package me.yswnthdev.rtdps.repository;

import me.yswnthdev.rtdps.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Integer> {
}