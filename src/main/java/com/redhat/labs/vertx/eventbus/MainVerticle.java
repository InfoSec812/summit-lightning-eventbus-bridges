package com.redhat.labs.vertx.eventbus;

import io.vertx.camel.CamelBridge;
import io.vertx.camel.CamelBridgeOptions;
import io.vertx.camel.InboundMapping;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.twitter.TwitterComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An example Verticle which demonstrates 2 of the Vert.x EventBus bridge implementations for both Apache Camel and
 * WebSockets.
 */
public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);

    /**
     * A simple implementation of {@link RouteBuilder} which attaches to Twitter's streaming API
     */
    private class TweetStreamer extends RouteBuilder {

        static final String TWITTER_URI = "twitter://streaming/filter?type=event&lang=en&keywords=";

        @Override
        public void configure() throws Exception {
            // Create a list of keywords we would like to watch for on Twitter.
            List<String> keywords = new ArrayList<>();
            keywords.add("#openinnovationlabs");
            keywords.add("#RHSummit");
            keywords.add("#RedHat");
            keywords.add("#Vertx");
            keywords.add("#infinispan");
            keywords.add("#openshift");
            keywords.add("#reactive");
            keywords.add("#openshift");
            JsonObject config = vertx.getOrCreateContext().config();

            TwitterComponent tc = getContext().getComponent("twitter", TwitterComponent.class);
            tc.setAccessToken(config.getString("twitter_access_token"));
            tc.setAccessTokenSecret(config.getString("twitter_access_secret"));
            tc.setConsumerKey(config.getString("twitter_consumer_key"));
            tc.setConsumerSecret(config.getString("twitter_consumer_secret"));
            LOG.debug(config.encodePrettily());

            // stream twitter search for new tweets
            String keywordList = keywords.stream().collect(Collectors.joining(","));
            String uri = TWITTER_URI + URLEncoder.encode(keywordList, "utf8");

            from(uri)
                    .filter(body().isNotNull())
                    .filter(body().isInstanceOf(Status.class))
                    .filter(simple("${body.retweet} == false"))
                    .transform(simple("${body.text}"))
                    .to("direct:tweets");
        }
    }

    @Override
    public void start() {
        CamelContext camel = new DefaultCamelContext();

        // Add the TwitterStreamer Route to the Camel context and start the Camel Context
        try {
            camel.addRoutes(new TweetStreamer());
            camel.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Create a bridge from Camel to the Vert.x EventBus
        CamelBridge.create(vertx, new CamelBridgeOptions(camel)
                .addInboundMapping(InboundMapping.fromCamel("direct:tweets").usePublish().toVertx("tweet.stream")))
                .start();

        // Create a Vert.x Router for mapping URIs to implementation logic.
        Router router = Router.router(vertx);

        // Build a set of bridge options to allow message on the WebSocket event bus bridge
        BridgeOptions options = new BridgeOptions()
                .addOutboundPermitted(new PermittedOptions().setAddress("tweet.stream"));

        // Attach the websocket event bus bridge handler to the `/eventbus/*` URI endpoint
        router.route("/eventbus/*").handler(SockJSHandler.create(vertx).bridge(options));

        // Static content handler pointing to the "webroot" contained in src/main/resources
        // Handles all other HTTP requests received and handles sending 404 if items are not found.
        router.route("/*").handler(StaticHandler.create("webroot").setCachingEnabled(false));

        // Create an HTTP server and attach our Router
        vertx.createHttpServer().requestHandler(router::accept).listen(8080);

        // Set up a consumer which will log the messages from Camel to the Vert.x EventBus
        vertx.eventBus().consumer("tweet.stream").handler(m -> LOG.info(m.body().toString()));
    }

}
