package org.apache.storm.topology.accelerate;


import java.io.Serializable;

/**
 * Created by Administrator on 2018/11/20.
 */
public class ConstantParameter implements Serializable {
    public DataType type; // 数据类型，是枚举类型
    public String value; // 值 用字符串的形式存储
    public ConstantParameter(DataType type,int value){
        this.type = type;
        this.value = String.valueOf(value);
    }


    public ConstantParameter(DataType type,long value){
        this.type = type;
        this.value = String.valueOf(value);
    }


    public ConstantParameter(DataType type,double value){
        this.type = type;
        this.value = String.valueOf(value);
    }

    public ConstantParameter(DataType type,float value){
        this.type = type;
        this.value = String.valueOf(value);
    }

    public ConstantParameter(DataType type,char value){
        this.type = type;
        this.value = String.valueOf(value);
    }

    public ConstantParameter(DataType type,boolean value){
        this.type = type;
        this.value = String.valueOf(value);
    }

}
