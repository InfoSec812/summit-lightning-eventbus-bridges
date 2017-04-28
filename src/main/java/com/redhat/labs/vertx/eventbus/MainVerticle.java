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

public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start() {
        CamelContext camel = new DefaultCamelContext();

        try {
            camel.addRoutes(new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    JsonObject config = vertx.getOrCreateContext().config();

                    TwitterComponent tc = getContext().getComponent("twitter", TwitterComponent.class);
                    tc.setAccessToken(config.getString("twitter_access_token"));
                    tc.setAccessTokenSecret(config.getString("twitter_access_secret"));
                    tc.setConsumerKey(config.getString("twitter_consumer_key"));
                    tc.setConsumerSecret(config.getString("twitter_consumer_secret"));
                    LOG.debug(config.encodePrettily());

                    // stream twitter search for new tweets
                    String uri = "twitter://streaming/filter?type=event&lang=en&keywords=" + URLEncoder.encode("#RHSummit,#RedHat,#Vertx,#infinispan,#openshift", "utf8");

                    from(uri)
                            .filter(body().isNotNull())
                            .filter(body().isInstanceOf(Status.class))
                            .throttle(1).timePeriodMillis(250)
                            .filter(simple("${body.retweet} == false"))
                            .transform(simple("${body.text}"))
                            .to("direct:tweets");
                }
            });
            camel.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        CamelBridge.create(vertx, new CamelBridgeOptions(camel).addInboundMapping(InboundMapping
                                        .fromCamel("direct:tweets").usePublish().toVertx("tweet.stream"))).start();

        Router router = Router.router(vertx);

        BridgeOptions options = new BridgeOptions();
        options.addOutboundPermitted(new PermittedOptions().setAddress("tweet.stream"));
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx).bridge(options);

        router.route("/eventbus/*").handler(sockJSHandler);

        // Static content handler pointing to the "webroot" contained in src/main/resources
        router.route("/*").handler(StaticHandler.create("webroot"));

        vertx.createHttpServer().requestHandler(router::accept).listen(8080);

        vertx.eventBus().consumer("tweet.stream").handler(m -> LOG.info(m.body().toString()));
    }

}
