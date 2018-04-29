<template>
  <q-layout
    ref="layout"
    view="lHh Lpr fff"
    :left-class="{'bg-grey-2': true}">
    <q-card inline style="width: 300px" v-for="tweet in tweetList">
      <q-card-title>
        <a :href="tweet.url">
        <img :src="tweet.user.img"/>
        <span class="handle">{{tweet.user.handle}}</span>
        </a>
      </q-card-title>
      <q-card-main v-html="tweet.body" v-linkified></q-card-main>
    </q-card>
  </q-layout>
</template>

<script>
import {
  QLayout,
  QCard,
  QCardTitle,
  QCardMain,
  QCardSeparator,
  QCardActions
} from 'quasar'
import axios from 'axios'
import EventBus from 'vertx3-eventbus-client'

export default {
  name: 'index',
  components: {
    QLayout,
    QCard,
    QCardTitle,
    QCardMain,
    QCardSeparator,
    QCardActions
  },
  data () {
    return {
      tweetList: [],
      eventBus: {},
      rest: {},
      baseURL: null
    }
  },
  methods: {
    handleTweet (error, msg) {
      if (error) {
        console.log(error)
      } else {
        this.tweetList.unshift(msg.body)
        if (this.tweetList.length > 24) {
          this.tweetList.pop()
        }
      }
    }
  },
  mounted () {
    this.baseURL = (window.location.href).replace(/#.*/, '')
    this.rest = axios.create({
      baseURL: this.baseURL,
      timeout: 1000
    })
    this.rest.get('/api/v1/tweets/recent')
      .then((resp) => {
        var tweets = JSON.parse(resp.data)
        for (var x = 0; x < tweets.length; x++) {
          this.handleTweet(null, tweets[x])
        }
      })
      .catch((err) => {
        console.log(err)
      })
    var eventBusAddr = (window.location.href).replace(/#.*/, 'eventbus/')

    var options = {
      vertxbus_reconnect_attempts_max: Infinity, // Max reconnect attempts
      vertxbus_reconnect_delay_min: 1000, // Initial delay (in ms) before first reconnect attempt
      vertxbus_reconnect_delay_max: 10000, // Max delay (in ms) between reconnect attempts
      vertxbus_reconnect_exponent: 2, // Exponential backoff factor
      vertxbus_randomization_factor: 0.5 // Randomization factor between 0 and 1
    }
    this.eventBus = new EventBus(eventBusAddr, options)
    this.eventBus.onopen = () => {
      this.eventBus.registerHandler('tweet.status', this.handleTweet)
    }
  }
}
</script>

<style lang="stylus">
.handler
  font-weight: bold
</style>
