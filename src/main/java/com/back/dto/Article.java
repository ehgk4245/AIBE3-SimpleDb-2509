package com.back.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class Article {
    private Long id;
    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;
    private String title;
    private String body;
    private boolean isBlind;
}
