package org.apache.storm.topology.accelerate;

import java.io.Serializable;

/**
 * Created by Administrator on 2018/10/27.
 */
public class TupleInnerDataType implements Serializable{
    public DataType type; // 数据类型，是枚举类型
    public boolean isArray; //是否是数组
    public int arraySize;  //数组大小

    public TupleInnerDataType(DataType datatype, boolean isArray, int arraySize) {
        this.type = datatype;
        this.isArray = isArray;
        this.arraySize = isArray ? arraySize : 1;
    }

    public TupleInnerDataType(DataType datatype) {
        this(datatype,false,1);
    }
}
