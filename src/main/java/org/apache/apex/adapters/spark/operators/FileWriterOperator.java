package org.apache.apex.adapters.spark.operators;

import com.datatorrent.api.Context.OperatorContext;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.apex.adapters.spark.io.WriteToFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
@DefaultSerializer(JavaSerializer.class)
public class FileWriterOperator extends BaseOperatorSerializable implements Serializable
{
    private String absoluteFilePath;
    public String successFilePath;
    public FileWriterOperator()
    {
    }

    @Override
    public DefaultInputPortSerializable getInputPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable getOutputPort() {
        return null;
    }

    @Override
    public DefaultInputPortSerializable getControlPort() {
        return null;
    }

    @Override
    public DefaultOutputPortSerializable<Boolean> getControlOut() {
        return null;
    }

    Logger log = LoggerFactory.getLogger(FileWriterOperator.class);
    @Override
    public void setup(OperatorContext context)
    {
        isSerialized =false;
    }
    private static boolean isSerialized;
    public final transient DefaultInputPortSerializable<Object> input = new DefaultInputPortSerializable<Object>()
    {
        @Override
        public void process(Object tuple)
        {
            if(!isSerialized) {
                log.info("tuple here is {}", tuple);
                WriteToFS.write(absoluteFilePath, tuple);
                isSerialized=true;
            }
        }
    };

    public void setSuccessFilePath(String path){
        this.successFilePath=path;
    }
    public void setAbsoluteFilePath(String absoluteFilePath)
    {
        this.absoluteFilePath = absoluteFilePath;
    }
}
