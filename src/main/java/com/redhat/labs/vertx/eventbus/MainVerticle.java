package com.redhat.labs.vertx.eventbus;

import com.shekhargulati.reactivex.twitter.TweetStream;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Future;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.handler.StaticHandler;
import io.vertx.reactivex.ext.web.handler.sockjs.SockJSHandler;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.conf.ConfigurationBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * An example Verticle which demonstrates 2 of the Vert.x EventBus bridge implementations for both Apache Camel and
 * WebSockets.
 */
public class MainVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(MainVerticle.class);
    public static final String HTTP_URL_REGEX = "(https?://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])";

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
    public void start(io.vertx.core.Future<Void> startFuture) {
        DeploymentOptions dOpts = new DeploymentOptions().setConfig(config());
        loadDbSchema()
                .andThen(vertx.rxDeployVerticle("com.redhat.labs.vertx.eventbus.TweetDAO", dOpts).toCompletable())
                .andThen(this.startHttpServer())
                .andThen(this.streamTweetUpdates())
                .doOnError(err -> startFuture.fail(err))
                .toSingle(() -> Single.just(Boolean.TRUE))
                .subscribe(b -> startFuture.complete());
    }

    Completable streamTweetUpdates() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        JsonObject config = vertx.getOrCreateContext().config();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey(config.getString("twitter_consumer_key"))
            .setOAuthConsumerSecret(config.getString("twitter_consumer_secret"))
            .setOAuthAccessToken(config.getString("twitter_access_token"))
            .setOAuthAccessTokenSecret(config.getString("twitter_access_secret"));
        EventBus eb = vertx.eventBus();

        String[] keywords = config.getJsonArray("keywords")
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(","))
                .split(",");
        TweetStream.of(cb.build(), keywords)
            .map(this::mapToJsonObject)
            .doOnNext(j -> LOG.info(j.getLong("id").toString()))
            .subscribe(j -> eb.publish("tweet.status", j));
        return Completable.complete();
    }

    Completable startHttpServer() {
        Router router = Router.router(vertx);

        BridgeOptions options = new BridgeOptions();
        options.setOutboundPermitted(Arrays.asList(new PermittedOptions().setAddress("tweet.status")));
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx).bridge(options);

        router.route("/eventbus/*").handler(sockJSHandler);
        router.route("/api/v1/tweets/recent").handler(this::handleRecentTweets);

        // Static content handler pointing to the "webroot" contained in src/main/resources
        // Handles all other HTTP requests received and handles sending 404 if items are not found.
        router.route("/*").handler(StaticHandler.create("webroot").setCachingEnabled(false));
        return vertx.createHttpServer().requestHandler(router::accept).rxListen(8080).toCompletable();
    }

    /**
     * Synchronous method to use Liquibase to load the database schema
     * @param f A {@link Future} to be completed when operation is done
     */
    Completable loadDbSchema() {
        return vertx.rxExecuteBlocking(this::asyncLoadSchema).toCompletable();
    }

    void asyncLoadSchema(Future<Boolean> f) {
        Connection conn = null;
        try {
            JsonObject dbCfg = vertx.getOrCreateContext().config().getJsonObject("db");
            Class.forName(dbCfg.getString("driver_class"));
            conn = DriverManager.getConnection(
                    dbCfg.getString("url"),
                    dbCfg.getString("user"),
                    dbCfg.getString("password"));
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(conn));
            Liquibase liquibase = new Liquibase("schema.xml", new ClassLoaderResourceAccessor(), database);
            liquibase.update(new Contexts(), new LabelExpression());
            f.complete(Boolean.TRUE);

        } catch (Exception e) {
            if (e.getCause().getLocalizedMessage().contains("already exists"))
                if (e.getCause() != null) {
                    f.complete(Boolean.TRUE);
                } else {
                    f.fail(e);
                }
            else {
                f.fail(e);
            }
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqle) {
                    // Ignore
                }
            }
        }
    }

    void handleRecentTweets(RoutingContext ctx) {
        vertx.eventBus().rxSend("tweet.recent", null)
                .flatMap(reply -> Single.just(reply.body()))
                .cast(JsonArray.class)
                .doOnError(err -> ctx.response().setStatusCode(500).setStatusMessage("INTERNAL SERVER ERROR").end())
                .subscribe(tweets -> ctx.response()
                                        .setStatusMessage("OK")
                                        .setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(tweets.encodePrettily()));
    }

    private JsonObject mapToJsonObject(Status s) {
        return new JsonObject()
            .put("body", s.getText())
            .put("id", s.getId())
            .put("url", String.format("https://twitter.com/%s/status/%d", s.getUser().getScreenName(), s.getId()))
            .put("user", new JsonObject()
                .put("handle", s.getUser().getScreenName())
                .put("img", s.getUser().getMiniProfileImageURL())
            );
    }

}
