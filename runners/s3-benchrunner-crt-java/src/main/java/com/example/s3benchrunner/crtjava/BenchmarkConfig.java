package com.example.s3benchrunner.crtjava;

import com.fasterxml.jackson.jr.ob.JSON;
import software.amazon.awssdk.crt.s3.ChecksumAlgorithm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

// POJO for benchmark config, loaded from JSON
class BenchmarkConfig {
    int maxRepeatCount;
    int maxRepeatSecs;
    ChecksumAlgorithm checksum;
    boolean filesOnDisk;
    ArrayList<TaskConfig> tasks;

    static BenchmarkConfig fromJson(String jsonFilepath) {
        var config = new BenchmarkConfig();
        Map<String, Object> json;

        try {
            String jsonString = Files.readString(Paths.get(jsonFilepath));
            json = JSON.std.mapFrom(jsonString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        int version = (int) json.get("version");
        if (version > 1) {
            Util.exitWithSkipCode("config version not supported");
        }

        config.maxRepeatCount = (int) json.getOrDefault("maxRepeatCount", 10);
        config.maxRepeatSecs = (int) json.getOrDefault("maxRepeatSecs", 600);

        String checksumStr = (String) json.get("checksum");
        if (checksumStr == null) {
            config.checksum = ChecksumAlgorithm.NONE;
        } else {
            config.checksum = ChecksumAlgorithm.valueOf(checksumStr);
        }

        config.filesOnDisk = (boolean) json.getOrDefault("filesOnDisk", true);

        config.tasks = new ArrayList<>();

        @SuppressWarnings("unchecked")
        var tasksJson = (List<Map<String, Object>>) json.get("tasks");
        for (Map<String, Object> taskJson : tasksJson) {
            var task = new TaskConfig();

            task.action = (String) taskJson.get("action");
            task.key = (String) taskJson.get("key");

            // size looks like "5GiB" or "10KiB" or "1" (bytes)
            String sizeStr = (String) taskJson.get("size");
            var sizeRegex = Pattern.compile("^([0-9]+)(GiB|MiB|KiB|)$");
            var sizeMatch = sizeRegex.matcher(sizeStr);
            if (!sizeMatch.matches()) {
                throw new RuntimeException("Invalid size: " + sizeStr);
            }

            task.size = Long.parseLong(sizeMatch.group(1));
            var sizeUnit = sizeMatch.group(2);
            switch (sizeUnit) {
                case "KiB" -> task.size = Util.bytesFromKiB(task.size);
                case "MiB" -> task.size = Util.bytesFromMiB(task.size);
                case "GiB" -> task.size = Util.bytesFromGiB(task.size);
                case "" -> {
                }
                default -> throw new RuntimeException("Invalid size unit:" + sizeUnit);
            }

            config.tasks.add(task);
        }

        return config;
    }

    long bytesPerRun() {
        long bytes = 0;
        for (var task : tasks) {
            bytes += task.size;
        }
        return bytes;
    }
}
