;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.scheduler.EvenScheduler
  (:use [org.apache.storm util log config])
  (:require [clojure.set :as set])
  (:import [org.apache.storm.scheduler IScheduler Topologies
                                       Cluster TopologyDetails WorkerSlot ExecutorDetails]
           (org.apache.storm.daemon.common Executor))
  (:gen-class
    :implements [org.apache.storm.scheduler.IScheduler]))

(defn sort-slots [all-slots]
  (let [split-up (sort-by count > (vals (group-by first all-slots)))]
    (apply interleave-all split-up)
    ))

(defn get-alive-assigned-node+port->executors [cluster topology-id]
  (let [existing-assignment (.getAssignmentById cluster topology-id)
        executor->slot (if existing-assignment
                         (.getExecutorToSlot existing-assignment)
                         {}) 
        executor->node+port (into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] executor->slot
                                           :let [executor [(.getStartTask executor) (.getEndTask executor)]
                                                 node+port [(.getNodeId slot) (.getPort slot)]]]
                                       {executor node+port}))
        alive-assigned (reverse-map executor->node+port)]
    alive-assigned))

(defn- schedule-general-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        all-executors (->> topology
                          .getExecutors
                           (map #(Executor. (.getStartTask %) (.getEndTask %) (.isAccExecutor %) (.isAssignedAccExecutor %)))
                          set)
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (.getNumWorkers topology)
                                (+ (count available-slots) (count alive-assigned)))
        reassign-slots (take (- total-slots-to-use (count alive-assigned))
                             (sort-slots available-slots))
        reassign-executors (sort #(compare (first %1) (first %2)) (filter (fn [^Executor executor]
                                           (let [alive-assigned-executors (set (apply concat (vals alive-assigned)))]
                                             (if (contains? alive-assigned-executors [(:start-task-id executor) (:last-task-id executor)])
                                               false
                                               true))) all-executors))
        reassignment (into {}
                           (map vector
                                reassign-executors
                                ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                (repeat-seq (count reassign-executors) reassign-slots)))]
    (when-not (empty? reassignment)
      (log-message "Available slots: " (pr-str available-slots))
      )
    reassignment))

;;当我们要选择的Workerslot的数量小于可以分配worker的supervisor数量时,即 total-slots-to-use小于supervisor-num时 调用此方法来选取workerSlot
(defn- choose-slots [total-slots-to-use can-assign-supervisor-num sorted-slots can-assign-supervisorid-to-available-devices]
  (let [available-slots (take can-assign-supervisor-num sorted-slots)
        sorted-supervisor-with-devices (sort-by last > can-assign-supervisorid-to-available-devices)
        assign-supervisors ((set (map #(first %) (take total-slots-to-use sorted-supervisor-with-devices))))
        assign-slots (filter #(contains? assign-supervisors (first %)) available-slots)]
    assign-slots))

;;给有devices的slots排序
(defn sort-slots-with-devices [slots-with-devices supervisorid-to-available-devices]
  (let [group-slots-with-devices-by-id (group-by #(first %) slots-with-devices)
        sorted-slots-with-devices (mapcat (fn [[k seq]]
                                         (let [devices-num (get supervisorid-to-available-devices k)]
                                           (take devices-num (repeat-seq devices-num seq)))) group-slots-with-devices-by-id)]
    sorted-slots-with-devices))
;;调度含有acc组件的topology
(defn- schedule-acc-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        all-executors-details (.getExecutors topology)      ;;collection
        all-acc-executors-details (.getAccBoltExecutors topology) ;;collection
        all-executors (->> all-executors-details
                           (map #(Executor. (.getStartTask %) (.getEndTask %) (.isAccExecutor %) (.isAssignedAccExecutor %)))
                           set)
        all-acc-executors (filter (fn [^Executor executor]
                                    (if-let [is-acc-executor (:is-acc-executor executor)]
                                      true
                                      false)) all-executors)                         ;;计算拓扑中定义好的acc-executors
        all-supervisorid-to-available-devices (into {} (.getAvailableFpgaDevices cluster))  ;;获取<supervisor-id,availableFpgaDeviceNumber>
        ;;获取具有可用的slots的supervisor
        can-assign-supervisor-ids (keys (into {} (.getAvailableSlotsMap cluster)) )
        num-supervisors (count can-assign-supervisor-ids)
        can-assign-supervisor-id-to-available-devices (filter (fn [[k v]] (contains? can-assign-supervisor-ids k)) all-supervisorid-to-available-devices)


        all-available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        all-sorted-slots ((sort-slots all-available-slots))
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (.getNumWorkers topology)   ;;计算可用的slots数量
                                (+ (count all-available-slots) (count alive-assigned)))

        new-assign-slots (if (> total-slots-to-use num-supervisors)
                       (take (- total-slots-to-use (count alive-assigned)) all-sorted-slots)
                       (choose-slots total-slots-to-use num-supervisors all-sorted-slots can-assign-supervisor-id-to-available-devices)
                       )


        assign-supervisors (set (map #(first %) new-assign-slots))
        assign-supervisor-id-to-available-devices (filter (fn [[k v]] (contains? assign-supervisors k)) can-assign-supervisor-id-to-available-devices)
        total-available-devices (sum (vals assign-supervisor-id-to-available-devices)) ;;计算cluster中可用的devices总数
        can-assign-acc-executors-num (min (count all-acc-executors) total-available-devices) ;;计算可以被调度为acc-executor的executor个数，因为一个device只能承载一个executor，可能device的总量小于acc-executor的数量
        can-assign-acc-executor-details (if (< can-assign-acc-executors-num (count all-acc-executors))
                                          (take can-assign-acc-executors-num all-acc-executors-details)
                                          all-acc-executors-details)
        _ (.setAccExecutorType topology can-assign-acc-executor-details) ;;设置可被调度为acc-executor的ExecutorDetails 的isAssignedAccExecutor属性为true
        can-assign-acc-executors (map #(Executor. (:start-task-id %) (:last-task-id %) (:is-acc-executor %) true) (if (< can-assign-acc-executors-num (count all-acc-executors))
                                                                                                                    (take can-assign-acc-executors-num all-acc-executors)
                                                                                                                    all-acc-executors))  ;;就按顺序取出可以被调度为acc-executor的executors 并且设置他的is-assigned-acc-executor属性为true
        general-executors (set/difference all-executors can-assign-acc-executors) ;;将acc-executors除掉剩下的就是正常调度的general-executors了


        assign-supervisors-with-devices (set (keys assign-supervisor-id-to-available-devices))
        assign-slots-with-devices (filter #(contains? assign-supervisors-with-devices (first %)) new-assign-slots)
        sorted-slots-with-devices (sort-slots-with-devices assign-slots-with-devices all-supervisorid-to-available-devices)
        assign-slots-without-devices (set/difference new-assign-slots assign-slots-with-devices)


        reassign-acc-executors (sort #(compare (first %1) (first %2)) (filter (fn [^Executor executor]
                                               (let [alive-assigned-executors (set (apply concat (vals alive-assigned)))]
                                                 (if (contains? alive-assigned-executors [(:start-task-id executor) (:last-task-id executor)])
                                                   false
                                                   true))) can-assign-acc-executors))
        reassign-general-executors (sort #(compare (first %1) (first %2)) (filter (fn [^Executor executor]
                                                   (let [alive-assigned-executors (set (apply concat (vals alive-assigned)))]
                                                     (if (contains? alive-assigned-executors [(:start-task-id executor) (:last-task-id executor)])
                                                       false
                                                       true))) general-executors))

        acc-executors-reassignment (into {}
                                         (map vector
                                              reassign-acc-executors
                                              ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                              sorted-slots-with-devices))
        general-executors-reassignment (into {}
                                             (map vector
                                                  reassign-general-executors
                                                  (repeat-seq (count reassign-general-executors) (sort-slots new-assign-slots))))

        total-assignment (into acc-executors-reassignment general-executors-reassignment)]
    (when-not (empty? total-assignment)
      (log-message "Total assignment: " (pr-str total-assignment)))
    total-assignment)
  )

(defn schedule-topologies-evenly [^Topologies topologies ^Cluster cluster]
  (let [needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  new-assignment (if (.isAccTopology topology)
                                   (schedule-acc-topology topology cluster)
                                   (schedule-general-topology topology cluster))
                  node+port->executors (reverse-map new-assignment)]]
      (doseq [[node+port executors] node+port->executors
              :let [^WorkerSlot slot (WorkerSlot. (first node+port) (last node+port))
                    executors (for [^Executor executor executors]
                                (ExecutorDetails. (int (:start-task-id executor)) (int (:last-task-id executor)) (:is-acc-executor executor) (:is-assigned-acc-executor executor)))]]
        (.assign cluster slot topology-id executors)))))


(defn -prepare [this conf]
  )

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (schedule-topologies-evenly topologies cluster))
