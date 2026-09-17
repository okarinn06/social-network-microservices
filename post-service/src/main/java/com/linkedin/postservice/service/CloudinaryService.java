package com.linkedin.postservice.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class CloudinaryService {

    private final Cloudinary cloudinary;

    public String uploadFile(MultipartFile file, String keyPrefix) {
        try{
            String publicId = keyPrefix + "/" + UUID.randomUUID();

            Map<?, ?> result = cloudinary.uploader().upload(
                    file.getBytes(),
                    ObjectUtils.asMap(
                            "public_id", publicId,
                            "resource_type", "auto",
                            "overwrite", false
                    )
            );

            String url = result.get("secure_url").toString();

            log.info("File uploaded successfully: {}", url);

            return url;

        }
        catch(Exception e){
            throw new RuntimeException(
                    "Failed to upload file " + e.getMessage(), e
            );
        }
    }

}
