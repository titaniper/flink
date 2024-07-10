package com.ecubelabs.configs;

import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.util.Arrays;

public class ConfigLoader {
    private ConfigService service;

    public ConfigLoader() {
        this.service = RetrofitClient.getGitHubService();
    }

    public String[] load() throws IOException {
        Call<String[]> call = service.getTopics("partitioned");

        try {
            Response<String[]> response = call.execute();
            if (response.isSuccessful()) {
                String[] list = response.body();
//                String result = Arrays.stream(list).reduce("", (acc, v) -> acc + v + "\n");
//                System.out.println(result);
                return list;
            } else {
                throw new Error("Request not successful: " + response.message());
            }
        } catch (IOException e) {
//            System.out.println("Request failed: " + e.getMessage());
            throw e;
        }
    }
}
