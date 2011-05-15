(ns balrog.netty
  (:use balrog.executor)
  (:import (java.util.concurrent Executors)
	   (java.net InetSocketAddress)
           (org.jboss.netty.channel Channels ChannelEvent ChannelHandler ChannelHandlerContext ChannelPipelineFactory ChannelStateEvent ChannelUpstreamHandler ChannelDownstreamHandler ExceptionEvent MessageEvent SimpleChannelHandler StaticChannelPipeline)
	   (org.jboss.netty.channel.socket.nio NioServerSocketChannelFactory)
	   (org.jboss.netty.bootstrap ServerBootstrap)
	   (org.jboss.netty.handler.codec.frame FrameDecoder Delimiters DelimiterBasedFrameDecoder)
	   (org.jboss.netty.util VirtualExecutorService)))

(set! *warn-on-reflection* true)

(defn channel-upstream-handler [f]
  (reify ChannelUpstreamHandler
    (handleUpstream [this context event]
		    (f context event))))

(defn channel-downstream-handler [f]
  (reify ChannelDownstreamHandler
    (handleDownstream [this context event]
		      (f context event))))


(defmacro def-netty-handler [& forms]
  (let [defaults ['(connect [ctx event] )
		  '(message [ctx event] )
		  '(exception [ctx event] )
		  '(disconnected [ctx event] )]
	handlers (reduce #(assoc %1 (first %2) (rest (rest %2))) {} forms)
	default-handlers (reduce #(assoc %1 (first %2) (rest (rest %2))) {} defaults)
	syms (merge default-handlers handlers)]
    (when-not (= (count forms) 4)
      (throw (IllegalArgumentException. "defhandler requires 4 methods")))
    `(reify org.jboss.netty.channel.ChannelUpstreamHandler
       (~'handleUpstream [~'this ~'ctx ~'generic-event]
	 (cond
	  (instance? org.jboss.netty.channel.ChannelStateEvent ~'generic-event)
	  (let [~(with-meta 'event {:tag 'org.jboss.netty.channel.ChannelStateEvent})
		(cast org.jboss.netty.channel.ChannelStateEvent ~'generic-event)]
	    (when (= (.getState ~'event) org.jboss.netty.channel.ChannelState/CONNECTED)
	      (if (.getValue ~'event)
		(do ~@(get syms 'connect) nil)
		(do ~@(get syms 'disconnected) nil))))

	  (instance? org.jboss.netty.channel.MessageEvent ~'generic-event)
	  (let [~(with-meta 'event {:tag 'org.jboss.netty.channel.MessageEvent})
		(cast org.jboss.netty.channel.MessageEvent ~'generic-event)]
	    (do ~@(get syms 'message) nil))

	  (instance? org.jboss.netty.channel.ExceptionEvent ~'generic-event)
	  (let [~(with-meta 'event {:tag 'org.jboss.netty.channel.ExceptionEvent})
		(cast org.jboss.netty.channel.ExceptionEvent ~'generic-event)]
	    (do ~@(get syms 'exception) nil)))))))

(defmacro defhandler1 [& forms]
  (let [defaults ['(connect [ctx event] (proxy-super channelConnected ctx event))
		  '(message [ctx event] (proxy-super messagedReceived ctx event))
		  '(exception [ctx event] (proxy-super exceptionCaught ctx event))
		  '(disconnected [ctx event] (proxy-super channelDisconnected ctx event))]
	handlers (reduce #(assoc %1 (first %2) (rest (rest %2))) {} forms)
	default-handlers (reduce #(assoc %1 (first %2) (rest (rest %2))) {} defaults)
	syms (merge default-handlers handlers)]
    `(proxy [SimpleChannelHandler] []
       (~'channelConnected [~(with-meta 'ctx {:tag 'org.jboss.netty.channel.ChannelHandlerContext})
			    ~(with-meta 'event {:tag 'org.jboss.netty.channel.ChannelStateEvent})]
	 (do ~@(get syms 'connect) ))
       (~'messageReceived [~(with-meta 'ctx {:tag 'org.jboss.netty.channel.ChannelHandlerContext})
			   ~(with-meta 'event {:tag 'org.jboss.netty.channel.MessageEvent})]
	 (do ~@(get syms 'message) ))
       (~'exceptionCaught [~(with-meta 'ctx {:tag 'org.jboss.netty.channel.ChannelHandlerContext})
			   ~(with-meta 'event {:tag 'org.jboss.netty.channel.ExceptionEvent})]
	 (do ~@(get syms 'exception) ))
       (~'channelDisconnected [~(with-meta 'ctx {:tag 'org.jboss.netty.channel.ChannelHandlerContext})
			       ~(with-meta 'event {:tag 'org.jboss.netty.channel.ChannelStateEvent})]
	 (do ~@(get syms 'disconnected) )))))

(defn decoder [f]
  (channel-upstream-handler
   (fn [^ChannelHandlerContext context ^ChannelEvent event]
     (if (instance? MessageEvent event)
       (let [^MessageEvent message-event (cast MessageEvent event)]
	 (when-let [decoded-message (f (.getMessage message-event))]
	   (Channels/fireMessageReceived context decoded-message (.getRemoteAddress message-event))))
       (.sendUpstream context event)))))

(defn encoder [f]
  (channel-downstream-handler
   (fn [^ChannelHandlerContext context ^ChannelEvent event]
     (if (instance? MessageEvent event)
       (let [^MessageEvent message-event (cast MessageEvent event)
	     encoded-message (f (.getMessage message-event))]
	 (Channels/write context (.getFuture message-event) encoded-message (.getRemoteAddress message-event)))
       (.sendDownstream context event)))))

(defn delimeter-based-decoder []
  (DelimiterBasedFrameDecoder. Integer/MAX_VALUE false (Delimiters/nulDelimiter)))

; TODO: can make this use StaticChannelPipeline
(defn pipeline [& named-handlers]
  (let [pipeline (Channels/pipeline)]
    (doseq [[name handler] (partition 2 named-handlers)]
      (.addLast pipeline name handler))
    pipeline))

(defn create-pipeline-factory [pipeline-fn]
  (reify ChannelPipelineFactory
    (getPipeline [this] (pipeline-fn))))

(defn set-channel-pipeline-factory [bootstrap pipeline-factory-fn]
  (.setPipelineFactory bootstrap (create-pipeline-factory pipeline-factory-fn)))

(defn virtual-executor [executor]
  (VirtualExecutorService. executor))

(defn nio-server-bootstrap []
  (let [boss-executor (virtual-executor clojure.lang.Agent/soloExecutor)
	worker-executor (virtual-executor clojure.lang.Agent/pooledExecutor)
	executor (Executors/newCachedThreadPool)]
    (ServerBootstrap.
     (NioServerSocketChannelFactory. executor executor))))

(defn bind [bootstrap port]
  (.bind bootstrap (InetSocketAddress. port)))

(defn create-nio-server [pipeline-fn port]
  (let [server-bootstrap (nio-server-bootstrap)]
    (set-channel-pipeline-factory server-bootstrap pipeline-fn)
    (bind server-bootstrap port)))