;
; Copyright (C) 2014  Ohm Data
;
;  This program is free software: you can redistribute it and/or modify
;  it under the terms of the GNU Affero General Public License as
;  published by the Free Software Foundation, either version 3 of the
;  License, or (at your option) any later version.
;
;  This program is distributed in the hope that it will be useful,
;  but WITHOUT ANY WARRANTY; without even the implied warranty of
;  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;  GNU Affero General Public License for more details.
;
;  You should have received a copy of the GNU Affero General Public License
;  along with this program.  If not, see <http://www.gnu.org/licenses/>.
;

(ns c5db.control
  (:import (c5db C5DB NioFileConfigDirectory)
           (java.nio.file Paths))
  (:use [clojure.java.shell :only [sh]]
        [clojure.java.io :only [file]]))

(def username (System/getProperty "user.name"))
(def inherited-class-path (System/getProperty "java.class.path"))
(def java-home (System/getProperty "java.home"))
(def java-bin (.getPath (file java-home "bin" "java")))
(def c5main-class (.getName C5DB))
(def os-x (.contains (System/getProperty "os.name") "OS X"))
(def linux (.contains (System/getProperty "os.name") "Linux"))

;;; Process 'management'
(def all-processes (atom {}))

(defn stash-new-process [nodeId process]
  (let [exist @all-processes
        new-map (assoc exist nodeId process)]
    (reset! all-processes new-map)))

(defn remove-process [node-id]
  (let [node-id-str (str node-id)
        exist @all-processes
        process (get exist node-id-str)
        new-map (dissoc exist node-id-str)]
    (.destroy process)
    (reset! all-processes new-map)))

(defn killall-process []
  (let [exist @all-processes]
    (doseq [x (vals exist)] (.destroy x))
    (reset! all-processes {})))

;;; Process configuration, command line, and start functions

(defn run-directory [node-id]
  "The full directory a node-id will run in
  eg: /tmp/${user}/c5-123456"
  (str "/tmp/" username "/" "c5-" node-id))

(defn slf4j-args [node-id]
  "Returns the slf4j default arguments, requires the run-path for logging"
  ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"
   (str "-Dorg.slf4j.simpleLogger.logFile=" (run-directory node-id) "/" node-id ".log")
   "-Dorg.slf4j.simpleLogger.showDateTime=true"])

(defn debug-args [node-id]
  "Providers the JVM debug arguments"
  ; if the node-id is too big, we probably have a problem, cant bind to huge addresses
  (let [address (+ 5000 node-id)]
    (str "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=" address)))

(defn run-logfile [node-id]
  "The full path to the logfile"
  (str (run-directory node-id) "/" node-id ".log"))

(defn run-c5db [node-id & log-level]
  "Runs a C5DB for the given node-id and log-level, defaults to 'debug' log"
  (let [node-id-str (str node-id)
        ;; TODO use this argument later.
        log-level-str (or log-level "debug")
        ; side effects here, otherwise slf4j will fail to log.
        create-run-dir (clojure.java.io/make-parents (run-logfile node-id-str))
        args (flatten [java-bin
                       (slf4j-args node-id-str)
                       (debug-args node-id)
                       "-cp"
                       inherited-class-path
                       c5main-class
                       node-id-str])
        process-builder (.inheritIO (ProcessBuilder. (list* args)))
        process (.start process-builder)]
    (stash-new-process node-id-str process)
    process
    ))

(defn tail-log [node-id]
  (let [node-id-str (str node-id)
        logfile (run-logfile node-id-str)]
    (cond
      os-x (sh "/usr/bin/open" logfile)
      linux (let [command ["x-terminal-emulator" "-e" "tail" "-f" logfile]
                  process-builder (.inheritIO (ProcessBuilder. (list* command)))]
              (.start process-builder)))))

(defn get-path [p & more]
  (Paths/get p (into-array String more)))

;;; Easy to use functions

(defn start [node-ids]
  "Start a bunch of c5 processes based on the seq of node ids provided. We will also open/tail the logfile"
  (doall (map run-c5db node-ids))
  (doall (map tail-log node-ids)))

(defn stop []
  (killall-process))

(defn kill [node-id]
  (let [mult #(doall (map remove-process node-id))
        sing #(remove-process node-id)]
    (cond
      (vector? node-id) (mult)
      (seq? node-id) (mult)
      :else (sing))))

(defn killall []
  "alias for killall-process"
  (killall-process))

(defn tail [node-id]
  (let [mult #(doall (map tail-log node-id))
        sing #(tail-log node-id)]
    (cond
      (vector? node-id) (mult)
      (seq? node-id) (mult)
      :else (sing))))


;;; Calling into c5 code such as ConfigDirectory, etc

(defn c5-cfg
  [run-name]
  (NioFileConfigDirectory. (get-path "/tmp" username run-name)))

(defn c5-any-runs? []
  "Simple check to see if there are any c5db run-dirs for the user"
  (.isDirectory (clojure.java.io/file "/tmp" username)))

(defn c5-run-names []
  (seq (.list (clojure.java.io/file "/tmp/" username))))

(defn c5-cluster-ids [run-paths]
  "takes a list of run directory fragments that are located in /tmp/${username}"
  (map
    (fn [apath]
      {apath
       (.getNodeId (c5-cfg apath))})
    run-paths))