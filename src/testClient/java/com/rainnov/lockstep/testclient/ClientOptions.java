package com.rainnov.lockstep.testclient;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

record ClientOptions(
    URI controlUrl,
    String apiKey,
    int playerCount,
    int frameCount,
    Duration messageTimeout,
    URI dataUriOverride,
    String matchId
) {

    private static final Set<String> SUPPORTED_OPTIONS = Set.of(
        "control-url",
        "api-key",
        "players",
        "frames",
        "timeout-seconds",
        "data-uri",
        "match-id"
    );

    static ClientOptions parse(String[] arguments, Map<String, String> environment) {
        Map<String, String> options = parseArguments(arguments);
        URI controlUrl = httpUri(value(
            options,
            environment,
            "control-url",
            "LOCKSTEP_TEST_CONTROL_URL",
            "http://localhost:8080"
        ), "control-url");
        String apiKey = value(
            options,
            environment,
            "api-key",
            "LOCKSTEP_API_KEY",
            null
        );
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException(
                "缺少 API Key，请设置 LOCKSTEP_API_KEY 或传入 --api-key=<value>"
            );
        }
        int players = positiveInt(value(
            options,
            environment,
            "players",
            "LOCKSTEP_TEST_PLAYERS",
            "2"
        ), "players");
        if (players > 128) {
            throw new IllegalArgumentException("players 不能大于 128");
        }
        int frames = positiveInt(value(
            options,
            environment,
            "frames",
            "LOCKSTEP_TEST_FRAMES",
            "120"
        ), "frames");
        int timeoutSeconds = positiveInt(value(
            options,
            environment,
            "timeout-seconds",
            "LOCKSTEP_TEST_TIMEOUT_SECONDS",
            "10"
        ), "timeout-seconds");
        String dataUriText = value(
            options,
            environment,
            "data-uri",
            "LOCKSTEP_TEST_DATA_URI",
            null
        );
        URI dataUri = dataUriText == null
            ? null
            : websocketUri(dataUriText, "data-uri");
        String matchId = value(
            options,
            environment,
            "match-id",
            "LOCKSTEP_TEST_MATCH_ID",
            defaultMatchId()
        );
        if (matchId.isBlank()) {
            throw new IllegalArgumentException("match-id 不能为空");
        }
        return new ClientOptions(
            controlUrl,
            apiKey,
            players,
            frames,
            Duration.ofSeconds(timeoutSeconds),
            dataUri,
            matchId
        );
    }

    static String usage() {
        return """
            用法：
              .\\gradlew.bat runFrameSyncClient
              .\\gradlew.bat runFrameSyncClient -PtestClientArgs="--players=2 --frames=120"

            参数：
              --control-url=<uri>       控制面地址，默认 http://localhost:8080
              --api-key=<value>         控制面 API Key，推荐改用 LOCKSTEP_API_KEY
              --players=<count>         模拟玩家数，默认 2
              --frames=<count>          验证的有效输入帧数，默认 120
              --timeout-seconds=<n>     单条消息等待上限，默认 10
              --data-uri=<ws-uri>       覆盖分配响应中的数据面地址
              --match-id=<id>           指定对局 ID，默认自动生成

            对应环境变量：
              LOCKSTEP_API_KEY、LOCKSTEP_TEST_CONTROL_URL、
              LOCKSTEP_TEST_PLAYERS、LOCKSTEP_TEST_FRAMES、
              LOCKSTEP_TEST_TIMEOUT_SECONDS、LOCKSTEP_TEST_DATA_URI、
              LOCKSTEP_TEST_MATCH_ID
            """;
    }

    private static Map<String, String> parseArguments(String[] arguments) {
        Map<String, String> options = new HashMap<>();
        for (String argument : arguments) {
            if (!argument.startsWith("--") || !argument.contains("=")) {
                throw new IllegalArgumentException(
                    "无法识别参数 " + argument + "，参数格式应为 --name=value"
                );
            }
            int separator = argument.indexOf('=');
            String name = argument.substring(2, separator);
            String value = argument.substring(separator + 1);
            if (!SUPPORTED_OPTIONS.contains(name)) {
                throw new IllegalArgumentException("不支持参数 --" + name);
            }
            if (value.isBlank()) {
                throw new IllegalArgumentException("--" + name + " 不能为空");
            }
            if (options.put(name, value) != null) {
                throw new IllegalArgumentException("--" + name + " 不能重复");
            }
        }
        return options;
    }

    private static String value(
        Map<String, String> options,
        Map<String, String> environment,
        String option,
        String environmentName,
        String defaultValue
    ) {
        String argumentValue = options.get(option);
        if (argumentValue != null) {
            return argumentValue;
        }
        String environmentValue = environment.get(environmentName);
        return environmentValue == null || environmentValue.isBlank()
            ? defaultValue
            : environmentValue;
    }

    private static int positiveInt(String value, String name) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new NumberFormatException();
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " 必须是正整数", error);
        }
    }

    private static URI httpUri(String value, String name) {
        URI uri = URI.create(value);
        if (!"http".equalsIgnoreCase(uri.getScheme())
            && !"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(name + " 必须使用 http:// 或 https://");
        }
        return uri;
    }

    private static URI websocketUri(String value, String name) {
        URI uri = URI.create(value);
        if (!"ws".equalsIgnoreCase(uri.getScheme())
            && !"wss".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(name + " 必须使用 ws:// 或 wss://");
        }
        return uri;
    }

    private static String defaultMatchId() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        return "frame-sync-" + Instant.now().getEpochSecond() + "-" + suffix;
    }
}
