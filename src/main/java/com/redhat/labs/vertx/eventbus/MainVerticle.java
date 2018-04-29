package com.redhat.labs.vertx.eventbus;

import com.shekhargulati.reactivex.twitter.TweetStream;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.StaticHandler;
import io.vertx.reactivex.ext.web.handler.sockjs.SockJSHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start() {
        Router router = Router.router(vertx);

        BridgeOptions options = new BridgeOptions();
        options.setOutboundPermitted(Arrays.asList(new PermittedOptions().setAddress("tweet.status")));
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx).bridge(options);

        router.route("/eventbus/*").handler(sockJSHandler);

        // Static content handler pointing to the "webroot" contained in src/main/resources
        router.route("/*").handler(StaticHandler.create("webroot").setCachingEnabled(false));

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        JsonObject config = vertx.getOrCreateContext().config();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey(config.getString("twitter_consumer_key"))
            .setOAuthConsumerSecret(config.getString("twitter_consumer_secret"))
            .setOAuthAccessToken(config.getString("twitter_access_token"))
            .setOAuthAccessTokenSecret(config.getString("twitter_access_secret"));
        EventBus eb = vertx.eventBus();

        String[] keywords = config.getJsonArray("keywords").getList()
                                    .stream().map(Object::toString)
                                    .collect(Collectors.joining(","))
                                    .toString()
                                    .split(",");
        TweetStream.of(cb.build(), keywords)
                .map(this::mapToJsonObject)
                .doOnNext(j -> LOG.info(j.encodePrettily()))
                .subscribe(j -> eb.send("tweet.status", j));
    }

    private JsonObject mapToJsonObject(Status s) {
        return new JsonObject()
                        .put("body", s.getText())
                        .put("id", s.getId())
                        .put("url", String.format("https://twitter.com/%s/status/%d", s.getUser().getScreenName(), s.getId()))
                        .put("user", new JsonObject()
                            .put("handle", s.getUser().getScreenName())
                            .put("img", s.getUser().getMiniProfileImageURL())
                            .put("url", s.getUser().getURL())
                        );
    }

}
