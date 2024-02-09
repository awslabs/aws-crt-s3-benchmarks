package com.example.s3benchrunner;

import com.google.gson.Gson;
import software.amazon.awssdk.crt.s3.ChecksumAlgorithm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

// POJO for benchmark config, loaded from JSON
public class BenchmarkConfig {
    public int version;
    public boolean filesOnDisk;
    public ChecksumAlgorithm checksum;
    public int maxRepeatCount;
    public int maxRepeatSecs;
    public ArrayList<TaskConfig> tasks;

    static BenchmarkConfig fromJson(String jsonFilepath) {
        String jsonString;
        try {
            jsonString = Files.readString(Paths.get(jsonFilepath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        BenchmarkConfig config = new Gson().fromJson(jsonString, BenchmarkConfig.class);

        if (config.version != 2) {
            Util.exitWithSkipCode("workload version not supported");
        }

        return config;
    }

    public long bytesPerRun() {
        long bytes = 0;
        for (var task : tasks) {
            bytes += task.size;
        }
        return bytes;
    }
}
