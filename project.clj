(defproject balrog "1.0.0-SNAPSHOT"
  :description "A simple STOMP-based message queue"
  :dependencies [;[org.clojure/clojure "1.3.0-alpha6"]
		 ;[org.clojure/tools.logging "0.1.2"]
		 [org.clojure/clojure "1.3.0-master-SNAPSHOT"]
                 [org.clojure.contrib/logging "1.3.0-SNAPSHOT"]
		 [org.slf4j/slf4j-api "1.6.1"]
		 [org.slf4j/slf4j-simple "1.6.1"]
                 [org.jboss.netty/netty "3.2.4.Final"]
                 [org.fusesource.hawtdispatch/hawtdispatch "1.2-SNAPSHOT"]
		 [org.clojure/data.finger-tree "0.0.1"]]
  :dev-dependencies [[swank-clojure "1.2.1"]]
  :repositories {"JBoss Public Maven2 Repository" "http://repository.jboss.org/nexus/content/groups/public/"
                 "Fusesource Maven2 Snapshots" "http://repo.fusesource.com/nexus/content/repositories/snapshots"}
  ;:source-path "src/main/clojure"
  ;:java-source-path "src/main/java"
  :main balrog.core)
