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
    (:import (c5db C5DB ConfigDirectory)
             (java.nio.file Paths))
    (:use [clojure.java.shell :only [sh]]
          [clojure.java.io :only [file]]))

(def username (System/getProperty "user.name"))
(def inherited-class-path (System/getProperty "java.class.path"))
(def java-home (System/getProperty "java.home"))
(def java-bin (.getPath (file java-home "bin" "java")))
(def c5main-class (.getName C5DB))

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

(defn run-dir-fragment [node-id]
    "returns a run-directory fragment
    eg: c5-12345"
    (str "c5-" node-id))

(defn run-directory [node-id]
    "The full directory a node-id will run in
    eg: /tmp/${user}/c5-123456"
    (str "/tmp/" username "/" (run-dir-fragment node-id)))


(defn slf4j-args [node-id]
    "Returns the slf4j default arguments, requires the run-path for logging"
    ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"
     (str "-Dorg.slf4j.simpleLogger.logFile=" (run-directory node-id) "/" node-id ".log")
     "-Dorg.slf4j.simpleLogger.showDateTime=true"])

(defn run-logfile [node-id]
    "The full path to the logfile"
    (str (run-directory node-id) "/" node-id ".log"))

(defn tail-log [node-id]
    (let [node-id-str (str node-id)
          logfile (run-logfile node-id-str)]
        (sh "/usr/bin/open" logfile)))

(defn run-c5db [node-id & log-level]
    "Runs a C5DB for the given node-id and log-level, defaults to 'debug' log"
    (let [node-id-str (str node-id)
          log-level-str (or log-level "debug")
          run-dir (run-directory node-id-str)
          ; this is just the top-level directory, eg 'c5-NODEID' excluding the /tmp/$user portion
          run-dir-frag (run-dir-fragment node-id-str)
          ; side effects here, otherwise slf4j will fail to log.
          create-run-dir (clojure.java.io/make-parents (run-logfile node-id-str))
          log-args (slf4j-args node-id-str)
          args (flatten [java-bin log-args "-cp" inherited-class-path c5main-class run-dir-frag node-id-str])
          process-builder (.inheritIO (ProcessBuilder. (list* args)))
          process (.start process-builder)]
        (stash-new-process node-id-str process)
        process
        ))

(defn get-path [p & more]
    (Paths/get p (into-array String more)))

;; Java compatability here

(defn c5-cfg
    [run-name]
    (ConfigDirectory. (get-path "/tmp" username run-name)))

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