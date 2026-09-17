package com.linkedin.userservice.service;

import com.linkedin.userservice.dto.AuthResponse;
import com.linkedin.userservice.dto.LoginRequest;
import com.linkedin.userservice.dto.RegisterRequest;
import com.linkedin.userservice.entity.User;
import com.linkedin.userservice.entity.UserRole;
import com.linkedin.userservice.repository.UserRepository;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.jsonwebtoken.Jwts;

@Service
@Slf4j
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final BCryptPasswordEncoder passwordEncoder;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${jwt.secret}")
    private String secretKey;

    @Value("${jwt.expiration}")
    private long jwtExpiration;

    @Value("${jwt.refresh-expiration}")
    private long refreshExpiration;

    private static final String USER_CREATED_TOPIC = "user.created";

    public AuthResponse register(RegisterRequest request){

        log.info("Registering user: {}", request.getEmail());

        if(userRepository.existsByEmail(request.getEmail())){
            throw new RuntimeException(
                    "Email already in use. Please try again later"
            );
        }

        User user = new User();
        user.setFirstName(request.getFirstName());
        user.setLastName(request.getLastName());
        user.setEmail(request.getEmail());
        user.setHeadLine(request.getHeadLine());
        user.setLocation(request.getLocation());
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setRole(UserRole.NORMAL_USER);

        User savedUser = userRepository.save(user);
        log.info("User registered: {}", savedUser.getId());

        // Publish user.created event
        // Search service consume this and indexes user
        Map<String, Object> userCreatedEvent = new HashMap<>();
        userCreatedEvent.put("userId", savedUser.getId());
        userCreatedEvent.put("firstName", savedUser.getFirstName());
        userCreatedEvent.put("lastName", savedUser.getLastName());
        userCreatedEvent.put("email", savedUser.getEmail());
        userCreatedEvent.put("headLine", savedUser.getHeadLine());
        userCreatedEvent.put("location", savedUser.getLocation());

        kafkaTemplate.send(USER_CREATED_TOPIC, savedUser.getId(), userCreatedEvent);

        log.info("user.created event published: {}", savedUser.getId());

        String token = generateToken(savedUser.getId(), savedUser.getEmail());

        return buildAuthResponse(savedUser, token);
    }

    public AuthResponse login(LoginRequest request){
        log.info("Logining user: {}", request.getEmail());

        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> new RuntimeException(
                        "User not found" + request.getEmail()
                ));

        // Bcrypt verify - compare raw password with stored hash
        if(!passwordEncoder.matches(
                request.getPassword(), user.getPassword()
        )){
            throw new RuntimeException("Invalid Credentials");
        }

        log.info("Login successful: {}", user.getId());

        // Generate JWT Token
        String token = generateToken(user.getId(), user.getEmail());

        return buildAuthResponse(user, token);
    }

    private String generateToken(String userId, String email){
        return Jwts.builder()
                .claim("userId", userId)
                .setSubject(email)
                .setIssuedAt(new Date())
                .setExpiration(new Date(
                        System.currentTimeMillis() + jwtExpiration
                ))
                .signWith(getSigninKey(), SignatureAlgorithm.HS256)
                .compact();
    }

    /**
     * Generate refresh token
     * Used to get a new access token when it expires
     * Server validates and return new token
     * Client sends refresh token to /auth/refresh endpoint
     * @param userId
     * @return
     */
    private String generateRefreshToken(String userId){
        return Jwts.builder()
                .claim("userId", userId)
                .setSubject(userId)
                .setIssuedAt(new Date())
                .setExpiration(new Date(
                        System.currentTimeMillis() + refreshExpiration
                ))
                .signWith(getSigninKey(), SignatureAlgorithm.HS256)
                .compact();
    }

    private Key getSigninKey(){
        byte[] keyBytes = Decoders.BASE64.decode(secretKey);
        return Keys.hmacShaKeyFor(keyBytes);
    }

    private AuthResponse buildAuthResponse(User user, String token){
        AuthResponse response = new AuthResponse();
        response.setAccessToken(token);
        response.setRefreshToken(
                generateRefreshToken(user.getId())
        );
        response.setUserId(user.getId());
        response.setEmail(user.getEmail());
        response.setFirstName(user.getFirstName());
        response.setLastName(user.getLastName());

        return response;
    }
}
