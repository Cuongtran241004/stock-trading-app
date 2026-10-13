package com.trading.user_service.service;

import com.trading.user_service.dto.AuthResponse;
import com.trading.user_service.dto.LoginRequest;
import com.trading.user_service.dto.RegisterRequest;
import com.trading.user_service.dto.UserResponse;
import com.trading.user_service.entity.User;
import com.trading.user_service.repository.UserRepository;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.Key;
import java.security.KeyPair;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String USER_REGISTERED_TOPIC = "user.registered";

    @Value("${jwt.secret}")
    private String secret;

    @Value("${jwt.expiration}")
    private long jwtExpiration;

    @Value("${jwt.refresh-expiration}")
    private long refreshExpiration;
    /**
     *
     * @param request
     * @return
     */
    public AuthResponse register(RegisterRequest request){
        log.info("Registering user: {}", request.getEmail());

        if(userRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException("Email already in use!" + request.getEmail());
        }

        User user = new User();
        user.setEmail(request.getEmail());
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setFirstName(request.getFirstName());
        user.setLastName(request.getLastName());
        user.setWalletBalance(
                request.getInitialDeposit() != null ?
                        request.getInitialDeposit()
                        : BigDecimal.valueOf(10000)
        );

        User savedUser = userRepository.save(user);

        log.info("Saved user: {}", savedUser);

        // Publish user.registered event
        Map<String, Object> userRegisteredEvent = new HashMap<>();
        userRegisteredEvent.put("userId", savedUser.getId());
        userRegisteredEvent.put("email", savedUser.getEmail());
        userRegisteredEvent.put("firstName", savedUser.getFirstName());
        userRegisteredEvent.put("lastName", savedUser.getLastName());
        userRegisteredEvent.put("walletBalance", savedUser.getWalletBalance());

        kafkaTemplate.send(USER_REGISTERED_TOPIC, savedUser.getId(), userRegisteredEvent);

        String token = generateToken(savedUser.getId(), savedUser.getEmail());

        String refreshToken = generateRefreshToken(savedUser.getEmail());


        return buildAuthResponse(savedUser, token, refreshToken);
    }

    /**
     *
     * @param request
     * @return
     */
    public AuthResponse login(LoginRequest request){
        log.info("Login user: {}", request.getEmail());

        //
        User user = userRepository
                .findByEmail(request.getEmail())
                .orElseThrow(() -> new RuntimeException(
                        "User not found: " + request.getEmail()
                ));

        // Check password
        if(!passwordEncoder.matches(request.getPassword(), user.getPassword())){
            throw  new RuntimeException("Wrong password!");
        }

        log.info("Logged in user: {}", user.getEmail());

        String token = generateToken(user.getId(), user.getEmail());

        String refreshToken = generateRefreshToken(user.getEmail());


        return buildAuthResponse(user, token, refreshToken);

    }


    public UserResponse getUserById(String userId){
        User user = userRepository
                .findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User is not found with id " + userId
                ));

        return mapToResponse(user);
    }


    /**
     * Add funds to wallet
     * @param userId
     * @param amount
     * @return
     */
    public UserResponse addFunds(String userId, BigDecimal amount){
        User user = userRepository
                .findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User is not found with id " + userId
                ));

        user.setWalletBalance(user.getWalletBalance().add(amount));
        User savedUser = userRepository.save(user);

        log.info("Funds added: {} to user: {}", amount, userId);

        return mapToResponse(user);
    }

    /**
     * Deduct funds from wallet - Called on BUY Order
     * @param userId
     * @param amount
     * @return
     */
    public UserResponse deductFunds(String userId, BigDecimal amount){
        User user = userRepository
                .findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User is not found with id " + userId
                ));

        if(user.getWalletBalance().compareTo(amount) < 0){
            throw new RuntimeException("Insufficient wallet balance!");
        }

        user.setWalletBalance(user.getWalletBalance().subtract(amount));
        User savedUser = userRepository.save(user);

        log.info("Funds deducted: {} to user: {}", amount, userId);

        return mapToResponse(user);
    }


    /**
     * Credit funds to wallet - Called on SELL Order
     * @param userId
     * @param amount
     * @return
     */
    public UserResponse creditFunds(String userId, BigDecimal amount){
        User user = userRepository
                .findById(userId)
                .orElseThrow(() -> new RuntimeException(
                        "User is not found with id " + userId
                ));

        user.setWalletBalance(user.getWalletBalance().add(amount));
        User savedUser = userRepository.save(user);

        log.info("Funds credited: {} to user: {}", amount, userId);

        return mapToResponse(user);
    }


    private String generateToken(String userId, String email){
        return Jwts.builder()
                .claim("userId", userId)
                .setSubject(email)
                .setIssuedAt(new Date())
                .setExpiration(new Date(
                        System.currentTimeMillis() + jwtExpiration))
                .signWith(getSignKey(), SignatureAlgorithm.HS256)
                .compact();
    }

    private String generateRefreshToken(String userId){
        return Jwts.builder()
                .claim("userId", userId)
                .setSubject(userId)
                .setIssuedAt(new Date())
                .setExpiration(new Date(
                        System.currentTimeMillis() + refreshExpiration))
                .signWith(getSignKey(), SignatureAlgorithm.HS256)
                .compact();
    }

    private Key getSignKey(){
        byte[] keyBytes = Decoders.BASE64.decode(secret);

        return Keys.hmacShaKeyFor(keyBytes);
    }

    private AuthResponse buildAuthResponse(User user, String token, String refreshToken){
        AuthResponse response = new AuthResponse();
        response.setAccessToken(token);
        response.setRefreshToken(refreshToken);
        response.setUserId(user.getId());
        response.setEmail(user.getEmail());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());
        response.setWalletBalance(user.getWalletBalance());

        return response;
    }
    /**
     *
     * @param user
     * @return
     */
    private UserResponse mapToResponse(User user){
        UserResponse response = new UserResponse();
        response.setId(user.getId());
        response.setEmail(user.getEmail());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());
        response.setWalletBalance(user.getWalletBalance());
        response.setStatus(user.getStatus());
        response.setCreatedAt(user.getCreatedAt());

        return  response;
    }


}
