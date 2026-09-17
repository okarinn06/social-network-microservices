package com.linkedin.postservice.service;

import com.linkedin.postservice.entity.Comment;
import com.linkedin.postservice.entity.Like;
import com.linkedin.postservice.entity.Post;
import com.linkedin.postservice.repository.CommentRepository;
import com.linkedin.postservice.repository.LikeRepository;
import com.linkedin.postservice.repository.PostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class PostService {

    private final PostRepository postRepository;
    private final LikeRepository likeRepository;
    private final CommentRepository commentRepository;
    private final CloudinaryService cloudinaryService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String POST_CREATED_TOPIC = "post.created";
    private static final String POST_LIKED_TOPIC = "post.liked";
    private static final String POST_COMMENTED_TOPIC = "post.commented";

    /**
     * Create a post
     * Optionally upload image to
     * Publish post.created event to Kafka
     * Feed Service and Search Service will consume this
     */
    public Post createPost(
            String authorId,
            String content,
            MultipartFile image
    ){
        log.info("Creating post for user: {}", authorId);

        Post post = new Post();
        post.setAuthorId(authorId);
        post.setContent(content);

        if(image != null && !image.isEmpty()) {
            String imageUrl = cloudinaryService.uploadFile(
                    image, "posts/" + authorId
            );
            post.setImageUrl(imageUrl);
        }

        Post savedPost = postRepository.save(post);

        log.info("Saved post: {}", savedPost.getId());

        // Publish to Kafka - Feed Service and Search service consume this
        Map<String, Object> postCreatedEvent = new HashMap<>();
        postCreatedEvent.put("postId", savedPost.getId());
        postCreatedEvent.put("authorId", savedPost.getAuthorId());
        postCreatedEvent.put("content", savedPost.getContent());
        postCreatedEvent.put("imageUrl", savedPost.getImageUrl());
        postCreatedEvent.put("createdAt", savedPost.getCreatedAt());

        kafkaTemplate.send(POST_CREATED_TOPIC, savedPost.getId(), postCreatedEvent);

        log.info("post.created event published: {}", savedPost.getId());

        return savedPost;
    }

    public Post getPost(String postId) {
        return postRepository.findById(postId)
                .orElseThrow(() -> new RuntimeException("Post not found"));
    }

    public List<Post> getUserPosts(String userId) {
        return postRepository
                .findByAuthorIdOrderByCreatedAtDesc(userId);
    }

    /**
     * Like or unlike a post
     * @param postId
     * @param userId
     * @return
     */
    public String likePost(String postId, String userId) {
        Post post = getPost(postId);

        if(likeRepository.existsByPostIdAndUserId(postId, userId)){

            // Unlike
            likeRepository.findByPostIdAndUserId(postId, userId)
                    .ifPresent(likeRepository::delete);
            post.setLikeCount(post.getLikeCount() - 1);
            postRepository.save(post);
            return "Post unliked";
        }

        // Like
        Like like = new Like();
        like.setPostId(postId);
        like.setUserId(userId);
        likeRepository.save(like);
        post.setLikeCount(post.getLikeCount() + 1);
        postRepository.save(post);

        // Publish post.liked event
        Map<String, Object> postLikedEvent = new HashMap<>();
        postLikedEvent.put("postId", postId);
        postLikedEvent.put("userId", userId);
        postLikedEvent.put("authorId", post.getAuthorId());

        kafkaTemplate.send(POST_LIKED_TOPIC, postId, postLikedEvent);

        return "Post liked";
    }

    /**
     * Add comment to post
     */
    public Comment addComment(
            String postId,
            String authorId,
            String content
    ){
        Post post = getPost(postId);

        Comment comment = new Comment();
        comment.setPostId(postId);
        comment.setAuthorId(authorId);
        comment.setContent(content);
        Comment savedComment = commentRepository.save(comment);

        post.setCommentCount(post.getCommentCount() + 1);
        postRepository.save(post);

        // Publish post.commented event
        Map<String, Object> postCommentedEvent = new HashMap<>();
        postCommentedEvent.put("postId", postId);
        postCommentedEvent.put("authorId", authorId);
        postCommentedEvent.put("commentId", savedComment.getId());
        postCommentedEvent.put("postAuthorId", post.getAuthorId());

        kafkaTemplate.send(POST_COMMENTED_TOPIC, postId, postCommentedEvent);

        return savedComment;
    }

    /**
     * Get comments for a post
     * @param postId
     * @return
     */
    public List<Comment> getComments(String postId) {
        return commentRepository
                .findByPostIdOrderByCreatedAtDesc(postId);
    }

    public void deletePost(String postId, String userId) {
        Post post = getPost(postId);

        if(!post.getAuthorId().equals(userId)){
            throw new RuntimeException("Not authorized to delete this post");
        }

        postRepository.delete(post);

        log.info("Deleted post: {}", postId);
    }

}
