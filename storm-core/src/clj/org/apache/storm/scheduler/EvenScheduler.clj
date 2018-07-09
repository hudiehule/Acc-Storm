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
  (let [split-up (sort-by count > (vals (group-by first all-slots)))] ;; split-up是一个按照supervisor的slots数量从大到小排序的slots集合的集合
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
        alive-assigned-executors (set (apply concat (vals alive-assigned)))
        reassign-executors (sort #(compare (:start-task-id %1) (:start-task-id %2)) (filter #(not (contains? alive-assigned-executors [(:start-task-id %) (:last-task-id %)])) all-executors))
        _ (log-message "hudie add reassign-executors")
        _ (map #(log-message "[" (:start-task-id %) " " (:last-task-id %) "]") reassign-executors)
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
;;这里我们选取了不同的supervisor上的total-slots-to-use个slots,每个supervisor上一个，选取的这些supervisor，满足它们上面的device num总和加起来最多
(defn- choose-slots [total-slots-to-use can-assign-supervisor-num sorted-slots can-assign-supervisorid-to-available-devices]
  (let [available-slots (take can-assign-supervisor-num sorted-slots)
        sorted-supervisor-with-devices (sort-by last > can-assign-supervisorid-to-available-devices)
        assign-supervisors (set (map #(first %) (take total-slots-to-use sorted-supervisor-with-devices)))
        assign-slots (filter #(contains? assign-supervisors (first %)) available-slots)]
    assign-slots))

;;给有devices的slots排序
(defn sort-slots-with-devices [slots-with-devices supervisorid-to-available-devices reassign-acc-executors]
  (let [group-slots-with-devices-by-id (group-by #(first %) slots-with-devices) ;;得到的是<suprvisor-id, slots的集合>这个map
        group-sorted-slots-with-devices (map (fn [[k seq]]
                                         (let [devices-num (get supervisorid-to-available-devices k)]
                                           [k (take devices-num (repeat-seq devices-num seq))])) group-slots-with-devices-by-id)
        split-up (sort-by count > (vals group-sorted-slots-with-devices))
        ]
    (apply interleave-all split-up)))

;;调度含有acc组件的topology
(defn- schedule-acc-topology [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)                       ;;得到这个拓扑的id
        all-executors-details (.getExecutors topology)      ;;collection  得到这个拓扑所有的executors的ExecutorDetail 包括了acc-executor的general executor形式
        all-acc-executors-details (.getAccBoltExecutors topology) ;;collection 得到这个拓扑所有的acc-executor的ExecutorDetail
        all-executors (->> all-executors-details
                           (map #(Executor. (.getStartTask %) (.getEndTask %) (.isAccExecutor %) (.isAssignedAccExecutor %)))
                           set)                             ;;将得到的executor转成common中定义的Executor形式
        _ (log-message "all-executor: " (pr-str all-executors))
        all-acc-executors (filter (fn [^Executor executor]
                                    (if-let [is-acc-executor (:is-acc-executor executor)]
                                      true
                                      false)) all-executors)                         ;;计算拓扑中定义好的acc-executors 转成common中定义的Executor形式
        _ (log-message "all-acc-executor: " (pr-str all-acc-executors))
        all-supervisorid-to-available-devices (into {} (.getAvailableFpgaDevices cluster))  ;;获取<supervisor-id,availableFpgaDeviceNumber>
        ;;获取具有可用的slots的supervisor
        can-assign-supervisor-ids (into {} (.getAvailableSlotsMap cluster))
        num-supervisors (count can-assign-supervisor-ids)   ;;可以启动worker的supervisor数量
        ;;获取获取<supervisor-id,availableFpgaDeviceNumber> 这里过滤掉了不可用的supervisor(所有的slots都被Worker占用了，不可分配了）
        can-assign-supervisor-id-to-available-devices (filter (fn [[k v]] (contains? can-assign-supervisor-ids k)) all-supervisorid-to-available-devices)


        all-available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %)))) ;;获取所有可用的slots ,slots的形式是[supervisor-id, port]
        all-sorted-slots (sort-slots all-available-slots)   ;;给slots排序
        _ (log-message "all-sorted-slots: " (pr-str all-sorted-slots))
        alive-assigned (get-alive-assigned-node+port->executors cluster topology-id) ;;得到的是<node+port,[start-task-id last-task-id]>的一个map
        alive-assigned-executors (set (apply concat (vals alive-assigned))) ;;这里的executor的形式是[start-task-id last-task-id]
        total-slots-to-use (min (.getNumWorkers topology)   ;;计算需要用到的slots数量
                                (+ (count all-available-slots) (count alive-assigned)))
        ;;接下来选取slots 也就是在哪些supervisor上的哪些port上启动worker 这里我们选取的策略是最大化可以使用的device数量 也就是说选取的这些slots它们所在的suprevisor
        ;;上的device 数量总和一定是最大的
        new-assign-slots (if (> total-slots-to-use num-supervisors)
                       (take (- total-slots-to-use (count alive-assigned)) all-sorted-slots)
                       (choose-slots total-slots-to-use num-supervisors all-sorted-slots can-assign-supervisor-id-to-available-devices)
                       )
        _ (log-message "new assign-slots: " (pr-str new-assign-slots))

        assign-supervisors (set (map #(first %) new-assign-slots)) ;;得到即将要分配slot(Worker)的supervisor-id
        ;;获取<supervisor-id, devicenum> 这里的supervisor-id是上面得到的要分配worker的supervisor的id
        assign-supervisor-id-to-available-devices (filter (fn [[k v]] (contains? assign-supervisors k)) can-assign-supervisor-id-to-available-devices)
        total-available-devices (sum (vals assign-supervisor-id-to-available-devices)) ;;计算cluster中可用的devices总数
        can-assign-acc-executors-num (min (count all-acc-executors) total-available-devices) ;;计算可以被调度为acc-executor的executor个数，因为一个device只能承载一个executor，可能device的总量小于acc-executor的数量
        can-assign-acc-executor-details (if (< can-assign-acc-executors-num (count all-acc-executors))
                                          (take can-assign-acc-executors-num all-acc-executors-details)
                                          all-acc-executors-details)
        _ (.setAccExecutorType topology can-assign-acc-executor-details) ;;设置可被调度为acc-executor的ExecutorDetails 的isAssignedAccExecutor属性为true
        can-assign-acc-executors (map #(Executor. (.getStartTask %) (.getEndTask %) (.isAccExecutor %) true) can-assign-acc-executor-details)  ;;就按顺序取出可以被调度为acc-executor的executors 并且设置他的is-assigned-acc-executor属性为true
        can-assign-acc-ececutors-general-format (map #(Executor. (.getStartTask %) (.getEndTask %) (.isAccExecutor %) false) can-assign-acc-executor-details)
        general-executors (set/difference all-executors can-assign-acc-ececutors-general-format) ;;将acc-executors除掉剩下的就是正常调度的general-executors了


        assign-supervisors-with-devices (set (keys assign-supervisor-id-to-available-devices))
        assign-slots-with-devices (set (filter #(contains? assign-supervisors-with-devices (first %)) new-assign-slots))
        ;;assign-slots-without-devices (set/difference (set new-assign-slots) assign-slots-with-devices)

        reassign-acc-executors (sort #(compare (:start-task-id %1) (:start-task-id %2)) (filter #(not (contains? alive-assigned-executors [(:start-task-id %) (:last-task-id %)])) can-assign-acc-executors))
        reassign-general-executors (sort #(compare (:start-task-id %1) (:start-task-id %2)) (filter #(not (contains? alive-assigned-executors [(:start-task-id %) (:last-task-id %)])) general-executors))
        _ (log-message "reassign-acc-executors: " + (pr-str reassign-acc-executors))
        _ (log-message "reassign-general-executors: " + (pr-str reassign-general-executors))

        sorted-slots-with-devices (sort-slots-with-devices assign-slots-with-devices all-supervisorid-to-available-devices reassign-acc-executors);;对具有device的slot进行排序
        _ (log-message "sorted-slots-with-devices: " + (pr-str sorted-slots-with-devices))
        acc-executors-reassignment (into {}
                                         (map vector
                                              reassign-acc-executors
                                              ;; for some reason it goes into infinite loop without limiting the repeat-seq
                                              sorted-slots-with-devices)) ;;这里acc-executors与sorted-slots-with-devices的个数是相等的
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
