package com.trading.user_service.repository;

import com.trading.user_service.entity.User;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import org.springframework.data.jpa.repository.JpaRepository;

import javax.swing.text.html.Option;
import java.util.Optional;

public interface UserRepository extends JpaRepository<User, String> {

    boolean existsByEmail(
            @NotBlank(message = "Email is required")
            @Email(message = "Invalid email format")
            String email);

    Optional<User> findByEmail(String email);
}
