/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.scheduler;

public class ExecutorDetails {
    int startTask;
    int endTask;



    boolean isAccExecutor;
    boolean isAssignedAccExecutor;

    public ExecutorDetails(int startTask, int endTask){
        this.startTask = startTask;
        this.endTask = endTask;
        this.isAccExecutor = false;
        this.isAssignedAccExecutor = false;
    }
    public ExecutorDetails(int startTask,int endTask,boolean isAccExecutor,boolean isAssignedAccExecutor){
        this.startTask = startTask;
        this.endTask = endTask;
        this.isAccExecutor = isAccExecutor;
        this.isAssignedAccExecutor = isAssignedAccExecutor;
    }

    public int getStartTask() {
        return startTask;
    }

    public int getEndTask() {
        return endTask;
    }

    public boolean isAccExecutor() {
        return isAccExecutor;
    }

    public boolean isAssignedAccExecutor(){return isAssignedAccExecutor;}

    public void setAccExecutor(boolean accExecutor) {
        isAccExecutor = accExecutor;
    }

    public void setAssignedExecutor(boolean assignedAccExecutor) {
        isAssignedAccExecutor = assignedAccExecutor;
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof ExecutorDetails)) {
            return false;
        }
        
        ExecutorDetails executor = (ExecutorDetails)other;
        return (this.startTask == executor.startTask) && (this.endTask == executor.endTask);
    }
    
    public int hashCode() {
        return this.startTask + 13 * this.endTask;
    }
    
    @Override
    public String toString() {
    	return "[" + this.startTask + ", " + this.endTask + ", isAccExecutor: "+this.isAccExecutor +", isAssignedAccExecutor: "+this.isAssignedAccExecutor +"]";
    }
}
