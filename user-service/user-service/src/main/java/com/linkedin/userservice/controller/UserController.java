package com.linkedin.userservice.controller;

import com.linkedin.userservice.dto.UserResponse;
import com.linkedin.userservice.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/api/v1/users")
@Slf4j
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    /**
     * Get user Profile
     * X-User-Id = requesting user (from Gateway)
     * userId in path = target user to fetch
     * @return
     */
    @GetMapping("/{userId}")
    public ResponseEntity<UserResponse> getUserProfile(
            @PathVariable String userId,
            @RequestHeader("X-User-Id") String requestingUserId
    ){
        log.info("Get profile: {} requested by: {}",
                userId, requestingUserId);

        return ResponseEntity.ok(
                userService.getUserProfile(userId)
        );
    }

    /**
     * Update own profile
     * User can only update their own profile
     * @param userId
     * @param requestingUserId
     * @return
     */
    @PutMapping("/{userId}/profile")
    public ResponseEntity<UserResponse> updateProfile(
            @PathVariable String userId,
            @RequestHeader("X-User-Id") String requestingUserId,
            @RequestBody UserResponse request
    ){
        if(!userId.equals(requestingUserId)){
            return ResponseEntity.status(403).build();
        }

        return ResponseEntity.ok(
                userService.updateProfile(userId, request)
        );
    }

    @PostMapping("/{userId}/profile-photo")
    public ResponseEntity<UserResponse> uploadProfilePhoto(
            @PathVariable String userId,
            @RequestHeader("X-User-Id") String requestingUserId,
            @RequestParam("file") MultipartFile file
    ){
        if(!userId.equals(requestingUserId)){
            return ResponseEntity.status(403).build();
        }

        return ResponseEntity.ok(
                userService.uploadProfilePhoto(userId, file)
        );
    }

    /**
     * Send connection request
     * Requester Id comes from X-User-Id header - already validated
     * @param targetUserId
     * @return
     */
    @PostMapping("/{targetUserId}/connect")
    public ResponseEntity<String> sendConnectionRequest(
            @PathVariable String targetUserId,
            @RequestHeader("X-User-Id") String requestingUserId
    ){
        return ResponseEntity.ok(
                userService.sendConnectionRequest(
                        targetUserId, requestingUserId
                )
        );
    }

    @PutMapping("/connection/{connectionId}/accept")
    public ResponseEntity<String> acceptConnection(
            @PathVariable String connectionId,
            @RequestHeader("X-User-Id") String requestingUserId
    ){
        return ResponseEntity.ok(
                userService.acceptConnectionRequest(connectionId)
        );
    }

    @GetMapping("/{userId}/connections")
    public ResponseEntity<List<UserResponse>> getConnections(
            @PathVariable String userId
    ){
        return ResponseEntity.ok(
                userService.getConnections(userId)
        );
    }
}
