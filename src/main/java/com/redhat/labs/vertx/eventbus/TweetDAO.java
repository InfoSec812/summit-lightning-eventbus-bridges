package com.redhat.labs.vertx.eventbus;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import io.vertx.reactivex.ext.sql.SQLClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class TweetDAO extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(TweetDAO.class);
    public static final String INSERT_TWEET = "INSERT INTO tweets (id, body, url, handle, img) VALUES (?, ?, ?, ?, ?);";

    SQLClient client;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        try {
            JsonObject dbConfig = config().getJsonObject("db");
            client = JDBCClient.createShared(vertx, dbConfig, "tweets");
            vertx.eventBus().consumer("tweet.status", this::saveTweet);
            vertx.eventBus().consumer("tweet.recent", this::recentTweets);
            startFuture.complete();
        } catch (Throwable t) {
            startFuture.fail(t);
        }
    }

    private void recentTweets(Message<Void> msg) {
        client.rxGetConnection()
              .flatMap(conn -> conn.rxQuery("SELECT * FROM tweets ORDER BY id LIMIT 40"))
              .map(rs -> rs.getRows())
              .map(this::mapToTweets)
              .doOnError(err -> msg.fail(1, err.getLocalizedMessage()))
              .subscribe(recentTweets -> msg.reply(recentTweets));
    }

    JsonArray mapToTweets(List<JsonObject> jsonObjects) {
        List<JsonObject> mappedJson = jsonObjects.stream()
                .map(j -> new JsonObject()
                    .put("body", j.getString("body"))
                    .put("id", j.getLong("id"))
                    .put("url", j.getString("url"))
                    .put("user", new JsonObject()
                        .put("handle", j.getString("handle"))
                        .put("img", j.getString("img"))
                    )
                )
                .collect(Collectors.toList());
        return new JsonArray(mappedJson);
    }

    void saveTweet(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        JsonArray params = new JsonArray()
                                    .add(body.getLong("id"))
                                    .add(body.getString("body"))
                                    .add(body.getString("url"))
                                    .add(body.getJsonObject("user").getString("handle"))
                                    .add(body.getJsonObject("user").getString("img"));
        client.rxGetConnection()
              .flatMap(conn -> conn.rxUpdateWithParams(INSERT_TWEET, params))
              .doOnError(err -> LOG.error("Unable to save tweet", err))
              .subscribe();
    }
}
