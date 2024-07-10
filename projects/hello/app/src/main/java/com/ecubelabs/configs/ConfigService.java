package com.ecubelabs.configs;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface ConfigService {
    @GET("/topics")
    Call<String[]> getTopics(@Query("keyword") String keyword);
}
