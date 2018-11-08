package org.apache.storm.topology.accelerate;

/**
 * Created by Administrator on 2018/11/7.
 */
public enum DataType {
    BYTE(0), SHORT(1), INT(2), LONG(3), FLOAT(4), DOUBLE(5), BOOLEAN(6), CHAR(7), STRING(8);
    public int dataTypeFlag;
    DataType(int i){
         dataTypeFlag = i;
    }
}
